import asyncio
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Union, List, Any

import aiohttp
import ntplib
import uvicorn
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, Depends
from pybit.unified_trading import HTTP
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base

from scripts import utils
from scripts.models import MarketData, Orderbook, MarketTicker, Trade, Position
from scripts.settings import ConfigSettings
from scripts.trade_status import TradeStatus
from scripts.utils import generate_signature

# Windows-specific asyncio policy
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Cargar configuración desde .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
config = ConfigSettings()

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("NertzMetalEngine")

# URLs de Bybit
BASE_URL = "https://api.bybit.com" if not config.USE_TESTNET else "https://api-testnet.bybit.com"
WS_URL = "wss://stream.bybit.com/v5/public/spot" if not config.USE_TESTNET else "wss://stream-testnet.bybit.com/v5/public/spot"

# Base de datos
DATABASE_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(DATABASE_DIR, exist_ok=True)
DATABASE_URL = os.path.join(DATABASE_DIR, 'trading.db')
engine = create_engine(f"sqlite:///{DATABASE_URL}", connect_args={"check_same_thread": False})
Base = declarative_base()

Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


async def get_server_time(session: aiohttp.ClientSession) -> int:
    url = f"{BASE_URL}/v5/market/time"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            server_time = int(data["result"]["timeNano"]) // 1_000_000
            local_time = int(time.time() * 1000)
            offset = server_time - local_time
            logger.info(f"✅ Bybit server time: {server_time}, offset: {offset} ms")
            return server_time
        logger.error(f"❌ Failed to fetch Bybit server time: {response.status}")
        return int(time.time() * 1000)


def get_ntp_time() -> int:
    try:
        client = ntplib.NTPClient()
        response = client.request('pool.ntp.org', timeout=2)
        ntp_time = int(response.tx_time * 1000)
        local_time = int(time.time() * 1000)
        offset = ntp_time - local_time
        logger.info(f"✅ NTP time: {ntp_time}, offset: {offset} ms")
        return ntp_time
    except Exception as e:
        logger.error(f"❌ NTP failed: {e}. Falling back to local time.")
        return int(time.time() * 1000)


async def get_synced_time() -> int:
    async with aiohttp.ClientSession() as session:
        try:
            server_time = await get_server_time(session)
            local_time = int(time.time() * 1000)
            offset = server_time - local_time
            adjusted_time = int(time.time() * 1000) + offset
            return adjusted_time
        except Exception as e:
            logger.error(f"❌ Error al sincronizar tiempo: {e}")
            # Fallback a NTP si falla el tiempo del servidor
            ntp_time = get_ntp_time()
            if ntp_time > 0:
                return ntp_time
            # Si todo falla, usar tiempo local con un offset de seguridad
            return int(time.time() * 1000) + 500


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def fetch_data(session: aiohttp.ClientSession, url: str, params: Optional[Dict[str, str]] = None) -> Optional[
    Dict]:
    async with session.get(url, params=params) as response:
        if response.status == 200:
            return await response.json()
        logger.error(f"❌ Error en {url}: {response.status}")
        return None


def _update_orderbook(bid_dict: Dict[str, float], ask_dict: Dict[str, float], data: Dict[str, Any]) -> None:
    for price_str, qty_str in data["data"]["b"]:
        price = float(price_str)
        qty = float(qty_str)
        if qty > 0:
            bid_dict[str(price)] = qty
        elif str(price) in bid_dict:
            del bid_dict[str(price)]
    for price_str, qty_str in data["data"]["a"]:
        price = float(price_str)
        qty = float(qty_str)
        if qty > 0:
            ask_dict[str(price)] = qty
        elif str(price) in ask_dict:
            del ask_dict[str(price)]


async def fetch_dict(session: aiohttp.ClientSession, url: str, params: Optional[Dict[str, str]] = None) -> \
        Optional[Dict[str, Any]]:
    async with session.get(url, params=params) as response:
        if response.status == 200:
            return await response.json()  # Esto devuelve un diccionario
        logger.error(f"❌ Error en {url}: {response.status}")
        return None


class NertzMetalEngine:
    def __init__(self) -> None:
        self.timeframe = config.TIMEFRAME
        self.symbols = config.SYMBOL.split(",")
        self.session_start = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        self.capital = 0.0
        self.initial_capital = 0.0
        self.positions = {symbol: [] for symbol in self.symbols}
        self.iterations = 0
        self.ws = None
        self.running = True
        self.paused = False
        self.orderbook_data = {symbol: {"bids": [], "asks": []} for symbol in self.symbols}
        self.ticker_data = {symbol: {"last_price": 0.0, "volume_24h": 0.0, "high_24h": 0.0, "low_24h": 0.0} for symbol
                            in self.symbols}
        self.candles = {symbol: [] for symbol in self.symbols}
        self.trade_id_counter = self._load_initial_trade_id()
        self.last_orderbook_log = 0
        self.last_trade_time = {symbol: datetime.min.replace(tzinfo=timezone.utc) for symbol in self.symbols}
        self.last_kline_time = {symbol: 0 for symbol in self.symbols}
        self.trade_buffer = {symbol: [] for symbol in self.symbols}
        self.buffer_size = 10
        self.error_count = 0
        self.max_errors = 10
        self.consecutive_errors = 0
        self.max_consecutive_errors = 5
        self.pause_duration = 300
        # Nuevos atributos para rastrear el estado de los resultados
        self.last_saved_total_trades = 0
        self.last_saved_capital = 0.0
        # Nuevos atributos para el caché de balance
        self.last_balance_update = datetime.min.replace(tzinfo=timezone.utc)
        self.cached_capital = 0.0
        self.cached_available_btc = 0.0
        self.balance_update_interval = timedelta(seconds=30)
        self._load_positions()

    async def validate_symbols(self) -> None:
        async with aiohttp.ClientSession() as session:
            for symbol in self.symbols[:]:
                try:
                    url = f"{BASE_URL}/v5/market/instruments-info"
                    params = {"category": "spot", "symbol": symbol}
                    response = await fetch_data(session, url, params)
                    if not response or "result" not in response or not response["result"]["list"]:
                        logger.error(f"❌ Símbolo {symbol} no es tradable en Bybit. Eliminando de la lista.")
                        self.symbols.remove(symbol)
                    else:
                        logger.info(f"✅ Símbolo {symbol} validado como tradable en Bybit.")
                except Exception as e:
                    logger.error(f"❌ Error al validar símbolo {symbol}: {e}. Eliminando de la lista.")
                    self.symbols.remove(symbol)
        if not self.symbols:
            logger.critical("❌ No hay símbolos válidos para operar. Deteniendo bot.")
            self.stop()

    @staticmethod
    def _load_initial_trade_id() -> int:
        with SessionLocal() as db:
            last_trade = db.query(Trade.trade_id).order_by(Trade.trade_id.desc()).first()
            return last_trade[0] + 1 if last_trade else 1

    def _load_positions(self) -> None:
        with SessionLocal() as db:
            for symbol in self.symbols:
                positions = db.query(Position).filter_by(symbol=symbol, status="open").all()
                self.positions[symbol] = [{
                    "order_id": p.order_id,
                    "symbol": p.symbol,
                    "action": p.action,
                    "entry_price": p.entry_price,
                    "quantity": p.quantity,
                    "timestamp": p.timestamp.isoformat(),
                    "tp": p.tp,
                    "sl": p.sl,
                    "status": p.status,
                    "profit_loss": 0.0  # Añadimos profit_loss por defecto
                } for p in positions]

    async def fetch_real_balance(self) -> float:
        try:
            session = HTTP(
                testnet=config.USE_TESTNET,
                api_key=config.BYBIT_API_KEY,
                api_secret=config.BYBIT_API_SECRET
            )
            timestamp = await get_synced_time()
            wallet_response = session.get_wallet_balance(
                accountType="UNIFIED",
                timestamp=str(timestamp),
                recvWindow=str(config.RECV_WINDOW)
            )
            if wallet_response.get("retCode") == 0:
                wallet = wallet_response["result"]["list"][0]["coin"]
                usdt_info = next((c for c in wallet if c["coin"] == "USDT"), None)
                if usdt_info:
                    wallet_balance = float(usdt_info.get("walletBalance", 0.0))
                    locked_balance = float(usdt_info.get("locked", 0.0))
                    available_balance = wallet_balance - locked_balance
                    logger.info(
                        f"💰 Saldo real USDT: {wallet_balance:.2f}, Locked: {locked_balance:.2f}, Disponible: {available_balance:.2f}")
                    return available_balance
                else:
                    logger.error(f"❌ No se encontró información de USDT en la respuesta: {wallet_response}")
                    return 1000.0
            else:
                logger.error(f"❌ Error al obtener saldo real: {wallet_response.get('retMsg')}")
                return 1000.0
        except Exception as e:
            logger.error(f"❌ Error al obtener saldo real: {e}")
            return 1000.0

    async def fetch_initial_data(self) -> None:
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_symbol_data(session, symbol) for symbol in self.symbols]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_symbol_data(self, session: aiohttp.ClientSession, symbol: str) -> None:
        try:
            kline_url = f"{BASE_URL}/v5/market/kline"
            params = {"category": "spot", "symbol": symbol, "interval": self.timeframe.replace("m", ""), "limit": "50"}
            kline_response = await fetch_data(session, kline_url, params)
            if kline_response and "result" in kline_response and "list" in kline_response["result"]:
                with SessionLocal() as db:
                    candles = [
                        MarketData(
                            timestamp=utils.timestamp_to_datetime(int(k[0])),
                            symbol=symbol,
                            open=float(k[1]),
                            high=float(k[2]),
                            low=float(k[3]),
                            close=float(k[4]),
                            volume=float(k[5]) if float(k[5]) > 0 else 0.001
                        ) for k in kline_response["result"]["list"]
                    ]
                    for candle in candles:
                        if not db.query(MarketData).filter_by(timestamp=candle.timestamp, symbol=symbol).first():
                            db.add(candle)
                    db.commit()

                    self.candles[symbol] = [
                        {
                            "timestamp": c.timestamp.isoformat(),
                            "symbol": c.symbol,
                            "open": c.open,
                            "high": c.high,
                            "low": c.low,
                            "close": c.close,
                            "volume": c.volume
                        } for c in candles
                    ]
                    logger.info(f"📈 Velas iniciales para {symbol}: {len(self.candles[symbol])} cargadas")
                    if all(c["volume"] == 0 for c in self.candles[symbol]):
                        logger.warning(f"⚠ Todas las velas iniciales para {symbol} tienen volumen 0 antes del ajuste")
                    if all(c["volume"] <= 0.001 for c in self.candles[symbol]):
                        logger.info(f"✅ Volumen ajustado a mínimo (0.001) para todas las velas de {symbol}")
                    else:
                        logger.info(
                            f"✅ Algunas velas de {symbol} tienen volumen real: {max(c['volume'] for c in self.candles[symbol])}")

            orderbook_url = f"{BASE_URL}/v5/market/orderbook"
            params = {"category": "spot", "symbol": symbol, "limit": "100"}
            orderbook_response = await fetch_data(session, orderbook_url, params)
            if orderbook_response and "result" in orderbook_response:
                self.orderbook_data[symbol] = {"bids": orderbook_response["result"]["b"],
                                               "asks": orderbook_response["result"]["a"]}
                logger.info(
                    f"📊 Orderbook inicial para {symbol}: Bids={len(self.orderbook_data[symbol]['bids'])}, Asks={len(self.orderbook_data[symbol]['asks'])}")
            else:
                logger.warning(f"⚠ No se obtuvo orderbook válido para {symbol}: {orderbook_response}")

            ticker_url = f"{BASE_URL}/v5/market/tickers"
            params = {"category": "spot", "symbol": symbol}
            ticker_response = await fetch_data(session, ticker_url, params)
            if ticker_response and "result" in ticker_response and "list" in ticker_response["result"]:
                ticker_data = ticker_response["result"]["list"][0]
                self.ticker_data[symbol] = {
                    "last_price": float(ticker_data["lastPrice"]),
                    "volume_24h": float(ticker_data["volume24h"]),
                    "high_24h": float(ticker_data["highPrice24h"]),
                    "low_24h": float(ticker_data["lowPrice24h"])
                }
                logger.info(f"⚡ Ticker inicial para {symbol}: Last={self.ticker_data[symbol]['last_price']}")
            else:
                logger.warning(f"⚠ No se obtuvo ticker válido para {symbol}: {ticker_response}")
        except Exception as e:
            logger.error(f"❌ Fetch inicial falló para {symbol}: {e}", exc_info=True)

    async def start_async(self) -> None:
        await self.validate_symbols()
        if not self.symbols:
            return

        self.capital = await self.fetch_real_balance()
        self.initial_capital = self.capital
        logger.info(f"🔥 Iniciando bot para {self.symbols} con capital inicial: {self.capital:.2f} USDT")
        max_attempts = 5
        for attempt in range(max_attempts):
            if not self.running:
                logger.info("🛑 Bot detenido antes de iniciar.")
                break
            try:
                logger.info(f"Intento {attempt + 1}/{max_attempts} para obtener datos iniciales...")
                await self.fetch_initial_data()
                logger.info("✅ Datos iniciales obtenidos, conectando al WebSocket...")
                await self._connect_websocket_async()
                break
            except Exception as e:
                logger.error(f"❌ Error al iniciar (intento {attempt + 1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(5)
                else:
                    logger.critical("❌ Máximo de intentos alcanzado. Deteniendo.")
                    self.stop()

    async def _connect_websocket_async(self) -> None:
        while self.running:
            try:
                async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=40) as ws:
                    self.ws = ws
                    logger.info("🌐 WebSocket abierto")
                    await self._resubscribe_async()
                    async for message in ws:
                        await self._on_message(ws, message)
            except websockets.ConnectionClosed as e:
                logger.warning(f"⚠️ WebSocket cerrado: {e}, intentando reconectar en 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"❌ Error en WebSocket: {e}")
                await asyncio.sleep(5)

    async def _authenticate_websocket(self, ws):
        timestamp = str(await get_synced_time())
        sign = generate_signature(config.BYBIT_API_SECRET, timestamp, config.BYBIT_API_KEY, str(config.RECV_WINDOW))
        auth_msg = {
            "op": "auth",
            "args": [config.BYBIT_API_KEY, timestamp, sign]
        }
        await ws.send(json.dumps(auth_msg))
        logger.info("🔑 Autenticación enviada al WebSocket")

    async def _resubscribe_async(self) -> None:
        interval = self.timeframe.replace("m", "")
        for symbol in self.symbols:
            subscription = {"op": "subscribe",
                            "args": [f"kline.{interval}.{symbol}", f"orderbook.50.{symbol}", f"tickers.{symbol}"]}
            if self.ws:
                await self.ws.send(json.dumps(subscription))
                logger.info(f"📡 Suscrito a {symbol}")

    async def _on_message(self, ws: Any, message: Any) -> None:
        if self.paused:
            logger.info("⏸ Bot en pausa, ignorando mensaje.")
            return

        with SessionLocal() as db:
            try:
                if isinstance(message, bytes):
                    message = message.decode('utf-8')
                elif isinstance(message, tuple):
                    message = message[0]
                elif message is None:
                    logger.warning("⚠️ Mensaje recibido es None, ignorando.")
                    return

                if isinstance(message, str):
                    data = json.loads(message)
                    logger.debug(f"📨 Mensaje procesado: {json.dumps(data, indent=2)}")

                    if "topic" not in data:
                        logger.debug("⚠️ Mensaje sin tema ('topic'), posiblemente ping/pong.")
                        if data.get("op") == "ping" and ws is not None:
                            await ws.send(json.dumps({"op": "pong", "ts": data.get("ts", int(time.time() * 1000))}))
                        return

                    symbol = data["topic"].split(".")[-1]
                    if symbol not in self.symbols:
                        logger.warning(f"⚠️ Símbolo desconocido: {symbol}")
                        return

                    if "kline" in data["topic"] and data.get("data") and len(data["data"]) > 0:
                        await self._handle_kline(symbol, data["data"][0], db)
                    elif "orderbook" in data["topic"] and data.get("data"):
                        await self._handle_orderbook(symbol, data, db)
                    elif "tickers" in data["topic"] and data.get("data"):
                        await self._handle_ticker(symbol, data["data"], db)
                        await self._execute_trade_on_ticker(symbol, db)
            except json.JSONDecodeError as e:
                logger.error(f"❌ Error de decodificación JSON: {e}")
            except Exception as e:
                logger.error(f"❌ Error inesperado en mensaje: {e}")
                self.error_count += 1
                self.consecutive_errors += 1
                if self.consecutive_errors >= self.max_consecutive_errors:
                    logger.warning(
                        f"⏸ Demasiados errores consecutivos ({self.consecutive_errors}). Pausando bot por {self.pause_duration} segundos.")
                    self.paused = True
                    await asyncio.sleep(self.pause_duration)
                    self.paused = False
                    self.consecutive_errors = 0
                if self.error_count >= self.max_errors:
                    logger.critical(f"❌ Demasiados errores ({self.error_count}). Deteniendo bot.")
                    self.stop()

    async def _handle_kline(self, symbol: str, kline: Dict, db: Session) -> None:
        try:
            timestamp_value = kline.get("start")
            if not timestamp_value or not str(timestamp_value).isdigit():
                logger.warning(f"⚠️ Timestamp inválido '{timestamp_value}' para {symbol}. Saltando.")
                return
            timestamp = utils.timestamp_to_datetime(int(timestamp_value))

            volume = float(kline.get("volume", 0)) if float(kline.get("volume", 0)) > 0 else 0.001
            open_price = float(kline.get("open", 0))
            high_price = float(kline.get("high", 0))
            low_price = float(kline.get("low", 0))
            close_price = float(kline.get("close", 0))

            logger.debug(f"📥 Kline recibido para {symbol}: timestamp={timestamp}, close={close_price}, volume={volume}")

            if not db.query(MarketData).filter_by(timestamp=timestamp, symbol=symbol).first():
                candle = MarketData(
                    timestamp=timestamp, symbol=symbol, open=open_price, high=high_price,
                    low=low_price, close=close_price, volume=volume
                )
                db.add(candle)
                db.commit()

                self.candles[symbol].append({
                    "timestamp": candle.timestamp.isoformat(),
                    "symbol": candle.symbol,
                    "open": candle.open,
                    "high": candle.high,
                    "low": candle.low,
                    "close": candle.close,
                    "volume": candle.volume
                })
                self.candles[symbol] = self.candles[symbol][-50:]
                self.last_kline_time[symbol] = int(timestamp_value)
                logger.info(f"⚡ Kline para {symbol}: Close={candle.close}, Volume={candle.volume}")
                logger.info(f"📈 Acumulados {len(self.candles[symbol])} velas para {symbol}")

                await self._execute_trade(symbol, db)
        except Exception as e:
            logger.error(f"❌ Error en _handle_kline para {symbol}: {e}")

    async def _handle_orderbook(self, symbol: str, data: Dict, db: Session) -> None:
        try:
            if data.get("type") == "snapshot":
                self.orderbook_data[symbol] = {"bids": data["data"]["b"], "asks": data["data"]["a"]}
                await self._store_orderbook(symbol, db)
                logger.info(
                    f"📊 Snapshot para {symbol}: Bids={len(self.orderbook_data[symbol]['bids'])}, Asks={len(self.orderbook_data[symbol]['asks'])}")
            elif data.get("type") == "delta":
                if symbol not in self.orderbook_data or not self.orderbook_data[symbol]["bids"]:
                    logger.warning(f"⚠️ No hay orderbook previo para {symbol}, esperando snapshot")
                    return
                current = self.orderbook_data[symbol]
                try:
                    # Convertir todos los valores a float antes de procesar
                    bid_dict = {float(b[0]): float(b[1]) for b in current["bids"]}
                    ask_dict = {float(a[0]): float(a[1]) for a in current["asks"]}

                    # Procesar el delta asegurando tipos numéricos
                    for price_str, qty_str in data["data"]["b"]:
                        price = float(price_str)
                        qty = float(qty_str)
                        if qty > 0:
                            bid_dict[price] = qty
                        elif price in bid_dict:
                            del bid_dict[price]

                    for price_str, qty_str in data["data"]["a"]:
                        price = float(price_str)
                        qty = float(qty_str)
                        if qty > 0:
                            ask_dict[price] = qty
                        elif price in ask_dict:
                            del ask_dict[price]

                    # Convertir de vuelta a strings solo para almacenamiento
                    self.orderbook_data[symbol] = {
                        "bids": [[str(p), str(q)] for p, q in sorted(bid_dict.items(), reverse=True) if q > 0][:50],
                        "asks": [[str(p), str(q)] for p, q in sorted(ask_dict.items()) if q > 0][:50]
                    }
                except (ValueError, TypeError) as e:
                    logger.error(f"❌ Error al convertir valores del orderbook para {symbol}: {e}")
                    return
                await self._store_orderbook(symbol, db)
                logger.info(
                    f"📊 Delta para {symbol}: Bids={len(self.orderbook_data[symbol]['bids'])}, Asks={len(self.orderbook_data[symbol]['asks'])}")
        except Exception as e:
            logger.error(f"❌ Error en _handle_orderbook para {symbol}: {e}")

    async def _store_orderbook(self, symbol: str, db: Session) -> None:
        try:
            orderbook = Orderbook(
                timestamp=datetime.now(timezone.utc),
                symbol=symbol,
                bids=json.dumps(self.orderbook_data[symbol]["bids"]),
                asks=json.dumps(self.orderbook_data[symbol]["asks"])
            )
            db.add(orderbook)
            db.commit()
            if time.time() - self.last_orderbook_log >= 5:
                logger.info(
                    f"🤘 Orderbook guardado para {symbol}: Bids={len(self.orderbook_data[symbol]['bids'])}, Asks={len(self.orderbook_data[symbol]['asks'])}")
                self.last_orderbook_log = time.time()
        except Exception as e:
            logger.error(f"❌ Error al guardar orderbook para {symbol}: {e}")

    async def _handle_ticker(self, symbol: str, ticker: Dict[str, Union[str, float]], db: Session) -> None:
        try:
            if not isinstance(ticker, dict) or not ticker:
                logger.warning(f"⚠️ Ticker inválido para {symbol}: {ticker}. Saltando.")
                return

            required = ["lastPrice", "volume24h", "highPrice24h", "lowPrice24h"]
            optional = ["usdIndexPrice"]
            if not all(key in ticker for key in required):
                logger.warning(f"⚠️ Faltan claves requeridas en ticker para {symbol}: {ticker}. Saltando.")
                return

            ticker_values = {}
            for key in required + optional:
                value = ticker.get(key, 0.0)
                try:
                    ticker_values[key] = float(value) if value else 0.0
                except (ValueError, TypeError):
                    ticker_values[key] = 0.0

            if ticker_values.get("usdIndexPrice", 0.0) <= 0:
                ticker_values["usdIndexPrice"] = self.ticker_data[symbol].get("usd_index_price", 0.0)

            self.ticker_data[symbol] = {
                "last_price": ticker_values["lastPrice"],
                "volume_24h": ticker_values["volume24h"],
                "high_24h": ticker_values["highPrice24h"],
                "low_24h": ticker_values["lowPrice24h"],
                "usd_index_price": ticker_values["usdIndexPrice"]
            }

            market_ticker = MarketTicker(
                timestamp=datetime.now(timezone.utc),
                symbol=symbol,
                last_price=self.ticker_data[symbol]["last_price"],
                volume_24h=self.ticker_data[symbol]["volume_24h"],
                high_24h=self.ticker_data[symbol]["high_24h"],
                low_24h=self.ticker_data[symbol]["low_24h"]
            )
            db.add(market_ticker)
            db.commit()

            logger.info(
                f"⚡ Ticker actualizado para {symbol}: Last={self.ticker_data[symbol]['last_price']}, USDIndex={self.ticker_data[symbol]['usd_index_price']}")
        except Exception as e:
            logger.error(f"❌ Error en _handle_ticker para {symbol}: {e}")

    @staticmethod
    def _determine_decision(symbol: str, metrics: Dict[str, Union[float, None]], available_btc: float) -> str:
        egm = float(metrics.get("egm") or 0.0)
        combined = float(metrics.get("combined") or 0.0)
        ild = float(metrics.get("ild") or 0.0)
        rol = float(metrics.get("rol") or 0.0)
        pio = float(metrics.get("pio") or 0.0)
        ogm = float(metrics.get("ogm") or 0.0)

        logger.info(
            f"🔍 {symbol}: EGM={egm:.4f}, Combined={combined:.4f}, ILD={ild:.4f}, ROL={rol:.4f}, PIO={pio:.4f}, OGM={ogm:.4f}, "
            f"Buy Threshold={config.EGM_BUY_THRESHOLD}, Sell Threshold={config.EGM_SELL_THRESHOLD}, Available BTC={available_btc:.8f}"
        )

        if available_btc >= config.MIN_TRADE_SIZE:
            if egm <= config.EGM_SELL_THRESHOLD or combined <= config.EGM_SELL_THRESHOLD:
                logger.info(f"📉 Señal de venta detectada para {symbol}: Combined={combined:.4f}, EGM={egm:.4f}")
                return "sell"

        if combined >= config.EGM_BUY_THRESHOLD and egm >= config.EGM_BUY_THRESHOLD:
            logger.info(f"📈 Señal de compra detectada para {symbol}: Combined={combined:.4f}, EGM={egm:.4f}")
            return "buy"

        return "hold"

    async def _cancel_stale_orders(self, symbol: str) -> None:
        try:
            session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
            timestamp = await get_synced_time()
            response = session.get_open_orders(category="spot", symbol=symbol, timestamp=str(timestamp),
                                               recvWindow=str(config.RECV_WINDOW))
            if response.get("retCode") == 0:
                current_time = datetime.now(timezone.utc)
                for order in response["result"]["list"]:
                    if not order.get("orderLinkId", "").startswith("nertz-"):
                        continue
                    order_time = utils.timestamp_to_datetime(int(order["createdTime"]))
                    if (current_time - order_time).total_seconds() > 60:  # Reducido de 300 a 60 segundos
                        cancel_response = session.cancel_order(category="spot", symbol=symbol, orderId=order["orderId"],
                                                               timestamp=str(timestamp),
                                                               recvWindow=str(config.RECV_WINDOW))
                        if cancel_response.get("retCode") == 0:
                            logger.info(f"✅ Orden {order['orderId']} cancelada por inactividad.")
                            with SessionLocal() as db:
                                position = db.query(Position).filter_by(order_id=order["orderId"]).first()
                                if position:
                                    position.status = "cancelled"
                                    db.commit()
                        else:
                            logger.error(
                                f"❌ Fallo al cancelar orden {order['orderId']}: {cancel_response.get('retMsg')}")
        except Exception as e:
            logger.error(f"❌ Error al cancelar órdenes inactivas para {symbol}: {e}")

    async def _execute_trade(self, symbol: str, db: Session) -> None:
        if self.paused:
            logger.info(f"⏸ Bot en pausa, ignorando trade para {symbol}.")
            return

        try:
            # Verificar capital mínimo
            if self.capital < 50.0:
                logger.critical(f"❌ Capital demasiado bajo ({self.capital:.2f} USDT). Deteniendo bot.")
                self.running = False
                return

            # Sincronizar posiciones con Bybit antes de cualquier acción
            session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
            timestamp = await get_synced_time()
            response = session.get_open_orders(category="spot", symbol=symbol, timestamp=str(timestamp),
                                               recvWindow=str(config.RECV_WINDOW))
            if response.get("retCode") == 0:
                open_orders = {order["orderId"]: order for order in response["result"]["list"]}
                with SessionLocal() as db:
                    for pos in self.positions[symbol]:
                        if pos["order_id"] not in open_orders and pos["status"] == "open":
                            pos["status"] = "closed"
                            position = db.query(Position).filter_by(order_id=pos["order_id"]).first()
                            if position:
                                position.status = "closed"
                                db.commit()
                                logger.info(f"Orden {pos['order_id']} marcada como cerrada tras sincronización.")
                    self.positions[symbol] = [pos for pos in self.positions[symbol] if pos["status"] == "open"]
            else:
                logger.error(f"Error al sincronizar órdenes: {response.get('retMsg')}")
                return

            # Calcular el drawdown ajustado
            total_equity = self.capital  # Solo usamos el capital disponible
            drawdown = (self.initial_capital - total_equity) / self.initial_capital if self.initial_capital > 0 else 0
            if drawdown >= config.MAX_DRAWDOWN:
                logger.critical(f"❌ Drawdown máximo alcanzado ({drawdown:.2%}). Deteniendo bot.")
                self.running = False
                return

            # Verificar el número de órdenes abiertas
            open_trades = [trade for trade in self.positions[symbol] if trade["status"] == "open"]
            max_open_orders = config.MAX_OPEN_ORDERS if config.MAX_OPEN_ORDERS is not None else 2
            while len(open_trades) >= max_open_orders:
                logger.warning(
                    f"⚠️ Máximo de órdenes abiertas alcanzado ({len(open_trades)}/{max_open_orders}) para {symbol}. Cancelando la orden más antigua...")
                oldest_trade = min(open_trades, key=lambda x: x["timestamp"])
                check_response = session.get_open_orders(
                    category="spot",
                    symbol=symbol,
                    orderId=oldest_trade["order_id"],
                    timestamp=str(timestamp),
                    recvWindow=str(config.RECV_WINDOW)
                )
                if check_response.get("retCode") == 0 and not check_response["result"]["list"]:
                    logger.info(f"Orden {oldest_trade['order_id']} ya no existe, marcando como cerrada.")
                    oldest_trade["status"] = "closed"
                    with SessionLocal() as db:
                        position = db.query(Position).filter_by(order_id=oldest_trade["order_id"]).first()
                        if position:
                            position.status = "closed"
                            db.commit()
                    self.positions[symbol] = [pos for pos in self.positions[symbol] if pos["status"] == "open"]
                else:
                    cancel_response = session.cancel_order(
                        category="spot",
                        symbol=symbol,
                        orderId=oldest_trade["order_id"],
                        timestamp=str(timestamp),
                        recvWindow=str(config.RECV_WINDOW)
                    )
                    if cancel_response.get("retCode") == 0:
                        logger.info(f"✅ Orden {oldest_trade['order_id']} cancelada para hacer espacio.")
                        oldest_trade["status"] = "cancelled"
                        oldest_trade["exit_price"] = None
                        oldest_trade["profit_loss"] = 0.0
                        with SessionLocal() as db:
                            trade = db.query(Trade).filter_by(order_id=oldest_trade["order_id"]).first()
                            if trade:
                                trade.exit_price = None
                                trade.profit_loss = 0.0
                                trade.decision = "cancelled"
                                db.commit()
                                await self._save_results(symbol, trade)
                        self.positions[symbol] = [pos for pos in self.positions[symbol] if pos["status"] == "open"]
                    else:
                        logger.error(
                            f"❌ Fallo al cancelar orden {oldest_trade['order_id']}: {cancel_response.get('retMsg')}")
                        return
                open_trades = [trade for trade in self.positions[symbol] if trade["status"] == "open"]

            # Forzar el cierre de posiciones abiertas si el precio cae significativamente
            last_price = self.ticker_data.get(symbol, {"last_price": 0.0})["last_price"]
            for trade in open_trades:
                if trade["action"].lower() == "buy":
                    entry_price = trade["entry_price"]
                    if last_price < entry_price * 0.99:  # Cierra si el precio cae un 1% por debajo del precio de entrada
                        logger.info(
                            f"📉 Precio cayó un 1% por debajo del precio de entrada para {symbol}: {last_price} < {entry_price * 0.99}")
                        close_result = await self._place_order(symbol, "sell", trade["quantity"], last_price, None,
                                                               None)
                        if close_result.get("success", False):
                            close_price = last_price
                            profit_loss = (close_price - entry_price) * trade["quantity"] * (1 - config.FEE_RATE)
                            trade["exit_price"] = close_price
                            trade["profit_loss"] = profit_loss
                            trade["status"] = "closed"
                            with SessionLocal() as db:
                                db_trade = db.query(Trade).filter_by(order_id=trade["order_id"]).first()
                                if db_trade:
                                    db_trade.exit_price = close_price
                                    db_trade.profit_loss = profit_loss
                                    db_trade.decision = "closed (price drop)"
                                    db_position = db.query(Position).filter_by(order_id=trade["order_id"]).first()
                                    if db_position:
                                        db_position.status = "closed"
                                    db.commit()
                            self.positions[symbol] = [pos for pos in self.positions[symbol] if
                                                      pos["order_id"] != trade["order_id"]]
                            self.capital += profit_loss
                            logger.info(
                                f"✅ Posición cerrada por caída de precio para {symbol}: P&L={profit_loss:.2f}, Capital={self.capital:.2f}")
                            await self._save_results(symbol, db_trade)
                        else:
                            logger.error(
                                f"❌ Fallo al cerrar posición por caída de precio para {symbol}: {close_result.get('message')}")

            candles = self.candles.get(symbol, [])
            if len(candles) < 5:
                logger.warning(f"⚠️ No hay suficientes velas ({len(candles)}/5) para {symbol}")
                return

            current_time = datetime.now(timezone.utc)
            cooldown = timedelta(seconds=config.DEFAULT_SLEEP_TIME)
            last_trade_time = self.last_trade_time.get(symbol, datetime.min.replace(tzinfo=timezone.utc))
            if current_time <= last_trade_time + cooldown:
                logger.debug(f"⏳ Cooldown activo para {symbol}, esperando hasta {last_trade_time + cooldown}")
                return

            await self._cancel_stale_orders(symbol)

            candle_data = [
                {"open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"], "volume": c["volume"]}
                for c in candles
            ]
            orderbook = self.orderbook_data.get(symbol, {"bids": [], "asks": []})
            ticker = self.ticker_data.get(symbol, {"last_price": 0.0})

            best_ask = float(orderbook["asks"][0][0]) if orderbook["asks"] else ticker["last_price"]
            best_bid = float(orderbook["bids"][0][0]) if orderbook["bids"] else ticker["last_price"]

            logger.info(
                f"📊 Datos de entrada para {symbol}: candles={len(candles)}, orderbook_bids={len(orderbook['bids'])}, ticker_last={ticker['last_price']}, best_ask={best_ask}, best_bid={best_bid}")

            metrics = utils.calculate_metrics(candle_data, orderbook, ticker)
            if all(v == 0.0 for v in metrics.values()):
                logger.warning(f"⚠️ Todas las métricas son 0 para {symbol}, saltando trade")
                return

            logger.info(
                f"📊 Métricas calculadas para {symbol}: pio={metrics.get('pio', 0):.4f}, ild={metrics.get('ild', 0):.4f}, egm={metrics.get('egm', 0):.4f}, rol={metrics.get('rol', 0):.4f}, combined={metrics.get('combined', 0):.4f}"
            )

            # Actualización forzada del balance real
            available_balance = await self.fetch_real_balance()
            self.cached_capital = available_balance
            session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY,
                           api_secret=config.BYBIT_API_SECRET)
            timestamp = await get_synced_time()
            wallet_response = session.get_wallet_balance(
                accountType="UNIFIED",
                timestamp=str(timestamp),
                recvWindow=str(config.RECV_WINDOW)
            )
            self.cached_available_btc = 0.0
            if wallet_response.get("retCode") == 0:
                wallet = wallet_response["result"]["list"][0]["coin"]
                btc_info = next((c for c in wallet if c["coin"] == "BTC"), None)
                if btc_info:
                    self.cached_available_btc = float(btc_info.get("walletBalance", 0.0))

            # Actualizar capital y registrar cambios
            previous_capital = self.capital
            self.capital = self.cached_capital
            capital_change = self.capital - previous_capital

            logger.info(
                f"💰 Saldo real USDT: {self.cached_capital:.2f}, Cambio: {capital_change:+.2f}, Disponible: {self.cached_capital:.2f}")
            logger.info(f"💰 Saldo disponible BTC: {self.cached_available_btc:.8f}")

            available_btc = self.cached_available_btc
            logger.info(f"💰 Capital disponible antes del trade: {self.capital:.2f} USDT")

            decision = self._determine_decision(symbol, metrics, available_btc)
            if decision == "hold":
                logger.debug(f"🤖 Decisión de hold para {symbol}")
                return
            elif decision == "sell" and available_btc < config.MIN_TRADE_SIZE:
                logger.warning(f"⚠️ Cantidad insuficiente de BTC ({available_btc:.8f}) para vender en {symbol}")
                return

            risk_per_trade = self.capital * config.RISK_FACTOR
            volatility = metrics.get("volatility", 0.01)
            if volatility <= 0:
                logger.warning(f"⚠️ Volatilidad inválida ({volatility}) para {symbol}")
                volatility = 0.01

            last_price = ticker.get("last_price", 0.0)
            if last_price <= 0:
                logger.warning(
                    f"⚠️ Precio inválido ({last_price}) para {symbol}, usando valor predeterminado de 75000.0")
                last_price = 75000.0
            logger.info(f"📈 Precio usado para {symbol}: {last_price}")

            quantity = (self.capital * config.RISK_FACTOR) / last_price
            quantity = round(quantity, 4)
            if decision == "sell":
                quantity = min(quantity, available_btc)
            quantity = max(min(quantity, config.MAX_TRADE_SIZE), config.MIN_TRADE_SIZE)
            logger.info(f"📊 Cantidad calculada para {symbol}: {quantity}")

            if decision == "sell" and quantity < config.MIN_TRADE_SIZE:
                logger.warning(f"⚠️ Cantidad insuficiente para venta ({quantity} BTC). Saltando trade.")
                return

            trade_value = quantity * last_price
            if decision == "buy" and trade_value > self.capital:
                logger.warning(f"⚠️ Cantidad excesiva ({trade_value:.2f}) para {symbol}. Ajustando...")
                quantity = (self.capital * 0.9) / last_price
                quantity = round(quantity, 4)
                if quantity < config.MIN_TRADE_SIZE:
                    quantity = config.MIN_TRADE_SIZE
                elif quantity > config.MAX_TRADE_SIZE:
                    quantity = config.MAX_TRADE_SIZE
                logger.info(f"📊 Cantidad ajustada para {symbol}: {quantity}")
                trade_value = quantity * last_price
                if trade_value > self.capital:
                    logger.warning(f"⚠️ Cantidad ajustada sigue siendo excesiva ({trade_value:.2f}). Saltando trade.")
                    return

            # Obtener el precio máximo permitido del error (90% del último precio)
            max_allowed_price = last_price * 0.997  # Factor de seguridad del 99.7%
            entry_price = min(best_ask,
                              max_allowed_price) if decision == "buy" else last_price  # Usar last_price para ventas
            logger.info(
                f"📈 Precio ajustado para {symbol}: {entry_price} (best_ask={best_ask}, best_bid={best_bid}, max_allowed={max_allowed_price:.2f})")

            market_data = {
                "close_prices": [c["close"] for c in candles],
                "best_ask": best_ask,
                "best_bid": best_bid
            }
            strategy = utils.TpslStrategy(fee_rate=config.FEE_RATE, max_drawdown=config.MAX_DRAWDOWN)
            signal = strategy.generate_signal(market_data, metrics)
            if signal["action"] != decision.upper():
                logger.warning(
                    f"⚠️ Decisión de estrategia ({signal['action']}) no coincide con la decisión del bot ({decision.upper()}). Usando decisión del bot.")

            # Ajustar TP/SL basado en la volatilidad y el precio máximo permitido
            tp = min(entry_price * 1.02, max_allowed_price * 1.015) if decision == "buy" else entry_price * 0.98
            sl = entry_price * 0.95 if decision == "buy" else entry_price * 1.05
            logger.info(f"📌 TP={tp:.2f} y SL={sl:.2f} calculados para {symbol}")

            fixed_timestamp = await get_synced_time()
            order_result = await self._place_order(symbol, decision, quantity, entry_price, tp, sl, fixed_timestamp)
            if not order_result.get("success", False):
                logger.error(
                    f"❌ Fallo al colocar orden para {symbol}: {order_result.get('message', 'Error desconocido')}")
                self.trade_id_counter += 1
                failed_trade = Trade(
                    trade_id=self.trade_id_counter - 1,
                    timestamp=current_time,
                    symbol=symbol,
                    action=decision,
                    entry_price=entry_price,
                    exit_price=None,
                    quantity=quantity,
                    profit_loss=0.0,
                    decision="failed",
                    combined=metrics.get("combined", 0.0),
                    ild=metrics.get("ild", 0.0),
                    egm=metrics.get("egm", 0.0),
                    rol=metrics.get("rol", 0.0),
                    pio=metrics.get("pio", 0.0),
                    ogm=metrics.get("ogm", 0.0),
                    risk_reward_ratio=2.0,
                    order_id=None
                )
                db.add(failed_trade)
                db.commit()
                await self._save_results(symbol, failed_trade)
                self.consecutive_errors += 1
                if self.consecutive_errors >= self.max_consecutive_errors:
                    logger.warning(
                        f"⏸ Demasiados errores consecutivos ({self.consecutive_errors}). Pausando bot por {self.pause_duration} segundos.")
                    self.paused = True
                    await asyncio.sleep(self.pause_duration)
                    self.paused = False
                    self.consecutive_errors = 0
                return

            order_id = order_result.get("order_id")
            if not order_id:
                logger.error(f"❌ No se obtuvo order_id para la orden de {symbol}")
                return

            max_retries = 5
            executed_qty = 0.0
            for attempt in range(max_retries):
                try:
                    order_status_response = session.get_order_history(
                        category="spot",
                        symbol=symbol,
                        orderId=order_id,
                        timestamp=str(fixed_timestamp),
                        recvWindow=str(config.RECV_WINDOW)
                    )
                    if order_status_response.get("retCode") == 0 and order_status_response["result"]["list"]:
                        order_info = order_status_response["result"]["list"][0]
                        order_status = order_info.get("orderStatus", "").lower()
                        executed_qty = float(order_info.get("cumExecQty", "0.0"))
                        if order_status in ["filled", "partiallyfilled"]:
                            logger.info(
                                f"✅ Orden {order_id} ejecutada: {order_status}, Cantidad ejecutada: {executed_qty}")
                            break
                        elif order_status in ["cancelled", "rejected"]:
                            logger.error(f"❌ Orden {order_id} no ejecutada: {order_status}")
                            return
                    else:
                        open_order_response = session.get_open_orders(
                            category="spot",
                            symbol=symbol,
                            orderId=order_id,
                            timestamp=str(fixed_timestamp),
                            recvWindow=str(config.RECV_WINDOW)
                        )
                        if open_order_response.get("retCode") == 0 and open_order_response["result"]["list"]:
                            order_info = open_order_response["result"]["list"][0]
                            order_status = order_info.get("orderStatus", "").lower()
                            executed_qty = float(order_info.get("cumExecQty", "0.0"))
                            if order_status in ["filled", "partiallyfilled"]:
                                logger.info(
                                    f"✅ Orden {order_id} ejecutada (vía get_open_orders): {order_status}, Cantidad ejecutada: {executed_qty}")
                                break
                            elif order_status in ["cancelled", "rejected"]:
                                logger.error(f"❌ Orden {order_id} no ejecutada (vía get_open_orders): {order_status}")
                                return
                        logger.warning(
                            f"⚠ Intento {attempt + 1}/{max_retries} fallido al verificar estado de la orden {order_id}: {order_status_response.get('retMsg', 'Respuesta vacía')}")
                except Exception as e:
                    logger.warning(
                        f"⚠ Intento {attempt + 1}/{max_retries} fallido al verificar estado de la orden {order_id}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                    logger.error(
                        f"❌ Fallo al verificar estado de la orden {order_id} después de {max_retries} intentos")
                    return

            if executed_qty == 0.0:
                logger.error(f"❌ Orden {order_id} no se ejecutó")
                return

            if executed_qty < quantity:
                logger.warning(f"⚠ Orden {order_id} ejecutada parcialmente: {executed_qty}/{quantity}")
                quantity = executed_qty

            if decision == "buy":
                self.capital -= trade_value * (1 + config.FEE_RATE)
            elif decision == "sell":
                self.capital += trade_value * (1 - config.FEE_RATE)

            self.trade_id_counter += 1

            trade = Trade(
                trade_id=self.trade_id_counter - 1,
                timestamp=current_time,
                symbol=symbol,
                action=decision,
                entry_price=entry_price,
                exit_price=None,
                quantity=quantity,
                profit_loss=None,
                decision=decision,
                combined=metrics.get("combined", 0.0),
                ild=metrics.get("ild", 0.0),
                egm=metrics.get("egm", 0.0),
                rol=metrics.get("rol", 0.0),
                pio=metrics.get("pio", 0.0),
                ogm=metrics.get("ogm", 0.0),
                risk_reward_ratio=2.0,
                order_id=order_id
            )
            db.add(trade)

            position = Position(
                order_id=order_id,
                symbol=symbol,
                action=decision,
                entry_price=entry_price,
                quantity=quantity,
                timestamp=current_time,
                tp=tp,
                sl=sl,
                status="open"
            )
            db.add(position)
            db.commit()

            position_dict = {
                "order_id": position.order_id,
                "symbol": position.symbol,
                "action": position.action,
                "entry_price": position.entry_price,
                "quantity": position.quantity,
                "timestamp": position.timestamp.isoformat(),
                "tp": position.tp,
                "sl": position.sl,
                "status": position.status,
                "profit_loss": 0.0
            }

            self.positions.setdefault(symbol, []).append(position_dict)
            self.trade_buffer[symbol].append(position_dict)
            self.last_trade_time[symbol] = current_time

            logger.info(
                f"💰 Orden colocada: {decision.upper()} {quantity:.6f} {symbol} @ {entry_price:.2f}, TP={tp:.2f}, SL={sl:.2f}, OrderID={trade.order_id}, Capital={self.capital:.2f}")

            asyncio.create_task(self._check_order_status(symbol, trade, position, quantity, side=decision))

            self.iterations += 1
            if config.MAX_ITERATIONS > 0 and self.iterations >= config.MAX_ITERATIONS:
                logger.info("🏁 Máximo de iteraciones alcanzado. Deteniendo bot.")
                self.running = False

            try:
                await self._save_results(symbol, trade)
            except Exception as e:
                logger.error(f"❌ Error al guardar resultados para {symbol}: {e}")
        except Exception as e:
            logger.error(f"❌ Error en _execute_trade para {symbol}: {e}")
            self.error_count += 1
            self.consecutive_errors += 1
            if self.consecutive_errors >= self.max_consecutive_errors:
                logger.warning(
                    f"⏸ Demasiados errores consecutivos ({self.consecutive_errors}). Pausando bot por {self.pause_duration} segundos.")
                self.paused = True
                await asyncio.sleep(self.pause_duration)
                self.paused = False
                self.consecutive_errors = 0
            if self.error_count >= self.max_errors:
                logger.critical(f"❌ Demasiados errores ({self.error_count}). Deteniendo bot.")
                self.running = False

    async def _execute_trade_on_ticker(self, symbol: str, db: Session) -> None:
        current_time = int(time.time() * 1000)
        if current_time - self.last_kline_time.get(symbol, 0) > 60000:
            logger.debug(f"🔄 Sin kline reciente para {symbol}, obteniendo datos del ticker")

            async with aiohttp.ClientSession() as session:
                ticker_url = f"{BASE_URL}/v5/market/tickers"
                params = {"category": "spot", "symbol": symbol}
                ticker_response = await fetch_data(session, ticker_url, params)
                if ticker_response and "result" in ticker_response and "list" in ticker_response["result"]:
                    ticker_data = ticker_response["result"]["list"][0]
                    self.ticker_data[symbol] = {
                        "last_price": float(ticker_data["lastPrice"]),
                        "volume_24h": float(ticker_data["volume24h"]),
                        "high_24h": float(ticker_data["highPrice24h"]),
                        "low_24h": float(ticker_data["lowPrice24h"])
                    }
                    logger.info(
                        f"⚡ Datos del ticker actualizados para {symbol}: Last={self.ticker_data[symbol]['last_price']}")
                else:
                    logger.warning(f"⚠ No se obtuvo ticker válido para {symbol}: {ticker_response}")

            # No llamamos a _save_results aquí, se manejará dentro de _execute_trade
            await self._execute_trade(symbol, db)

    async def _place_order(self, symbol: str, action: str, quantity: float, price: float, tp: float, sl: float,
                           fixed_timestamp: int = None) -> dict[str, Union[str, bool]]:
        global available_btc, available_usdt
        max_retries = 3
        base_recv_window = config.RECV_WINDOW
        current_recv_window = base_recv_window
        max_delay = 15
        session = None
        insufficient_balance = False
        last_error = None

        if not symbol or not isinstance(symbol, str):
            logger.error(f"❌ Símbolo inválido: {symbol}")
            return {"success": False, "order_id": None, "message": "Símbolo inválido"}
        if action.lower() not in ["buy", "sell"]:
            logger.error(f"❌ Acción inválida: {action}")
            return {"success": False, "order_id": None, "message": "Acción debe ser 'buy' o 'sell'"}
        if quantity <= 0 or not isinstance(quantity, (int, float)):
            logger.error(f"❌ Cantidad inválida: {quantity}")
            return {"success": False, "order_id": None, "message": "Cantidad debe ser positiva"}
        if price <= 0 and config.ORDER_TYPE.lower() == "limit":
            logger.error(f"❌ Precio inválido: {price}")
            return {"success": False, "order_id": None, "message": "Precio debe ser positivo para órdenes limit"}

        # Obtener el precio actual del mercado
        last_price = self.ticker_data.get(symbol, {"last_price": 0.0})["last_price"]
        if last_price <= 0:
            logger.error(f"❌ Precio actual inválido para {symbol}: {last_price}")
            return {"success": False, "order_id": None, "message": "Precio actual inválido"}

        for attempt in range(max_retries):
            if insufficient_balance:
                logger.error(f"❌ Saldo insuficiente detectado, cancelando reintentos para {symbol}")
                self.error_count += 1
                self.consecutive_errors += 1
                if self.consecutive_errors >= self.max_consecutive_errors:
                    logger.warning(
                        f"⏸ Demasiados errores consecutivos ({self.consecutive_errors}). Pausando bot por {self.pause_duration} segundos.")
                    self.paused = True
                    await asyncio.sleep(self.pause_duration)
                    self.paused = False
                    self.consecutive_errors = 0
                if self.error_count >= self.max_errors:
                    logger.critical(f"❌ Demasiados errores ({self.error_count}). Deteniendo bot.")
                    self.stop()
                return {"success": False, "order_id": None, "message": "Saldo insuficiente, reintentos cancelados"}
            try:
                if not config.BYBIT_API_KEY or not config.BYBIT_API_SECRET:
                    logger.warning(f"⚠️ Sin API keys, simulando orden: {action.upper()} {quantity:.6f} {symbol}")
                    return {"success": True, "order_id": f"sim-{self.trade_id_counter}", "message": "Orden simulada"}

                # Incrementar recv_window exponencialmente con cada reintento
                recv_window = base_recv_window * (2 ** attempt)
                # Obtener timestamp sincronizado
                timestamp = fixed_timestamp if fixed_timestamp else await get_synced_time()
                logger.info(f"📅 Timestamp usado: {timestamp}, recv_window: {current_recv_window}")
                # Agregar un pequeño offset aleatorio para evitar colisiones
                timestamp += random.randint(1, 100)

                session = HTTP(
                    testnet=config.USE_TESTNET,
                    api_key=config.BYBIT_API_KEY,
                    api_secret=config.BYBIT_API_SECRET
                )

                for retry in range(3):
                    try:
                        wallet_response = session.get_wallet_balance(accountType="UNIFIED", timestamp=str(timestamp),
                                                                     recvWindow=str(recv_window))
                        logger.debug(f"📥 Respuesta de wallet: {json.dumps(wallet_response, indent=2)}")
                        if wallet_response.get("retCode") != 0:
                            raise Exception(f"Error en balance: {wallet_response.get('retMsg')}")
                        wallet = wallet_response["result"]["list"][0]["coin"]
                        available_usdt = float(next((c["walletBalance"] for c in wallet if c["coin"] == "USDT"), 0.0))
                        locked_usdt = float(next((c["locked"] for c in wallet if c["coin"] == "USDT"), 0.0))
                        available_usdt -= locked_usdt
                        available_btc = float(next((c["walletBalance"] for c in wallet if c["coin"] == "BTC"), 0.0))
                        break
                    except Exception as e:
                        logger.warning(f"⚠ Intento {retry + 1}/3 fallido al obtener balance: {e}")
                        if retry < 2:
                            await asyncio.sleep(2)
                        else:
                            raise Exception(f"No se pudo obtener el balance: {str(e)}")

                if action.lower() == "buy" and (quantity * price > available_usdt):
                    insufficient_balance = True
                    return {"success": False, "order_id": None,
                            "message": f"Saldo USDT insuficiente: {available_usdt:.2f}, requerido: {quantity * price:.2f}"}
                elif action.lower() == "sell" and (quantity > available_btc):
                    insufficient_balance = True
                    return {"success": False, "order_id": None,
                            "message": f"Saldo BTC insuficiente: {available_btc:.6f}, requerido: {quantity:.6f}"}

                side = "Buy" if action.lower() == "buy" else "Sell"
                order_type = config.ORDER_TYPE.lower()
                time_in_force = config.TIME_IN_FORCE

                order_link_id = f"nertz-{self.trade_id_counter}-{int(time.time() * 1000)}"

                # Usar el precio actual del mercado para ventas
                order_price = price if action.lower() == "buy" else last_price

                order_params = {
                    "category": "spot",
                    "symbol": symbol,
                    "side": side,
                    "orderType": order_type.upper(),
                    "qty": f"{quantity:.6f}",
                    "timeInForce": time_in_force.upper(),
                    "timestamp": str(timestamp),
                    "recvWindow": str(current_recv_window),
                    "orderLinkId": order_link_id
                }
                if order_type == "limit":
                    order_params["price"] = f"{order_price:.2f}"

                logger.debug("⚠ TP/SL no se incluirán en la orden spot; se manejarán manualmente.")

                response = session.place_order(**order_params)
                logger.debug(f"📥 Respuesta de pybit: {json.dumps(response, indent=2)}")

                if response.get("retCode") == 0:
                    order_id = response["result"]["orderId"]
                    logger.info(
                        f"✅ Orden colocada: {symbol} {side} {quantity:.6f} @ {order_price if order_type.lower() == 'limit' else 'market'}, OrderID={order_id}, OrderLinkId={order_link_id}")
                    self.consecutive_errors = 0
                    return {"success": True, "order_id": order_id, "message": "Orden exitosa"}
                else:
                    logger.error(
                        f"❌ Error al colocar orden: retCode={response.get('retCode')}, retMsg={response.get('retMsg')}")
                    if "Insufficient balance" in response.get('retMsg', ''):
                        insufficient_balance = True
                    raise Exception(f"retCode: {response.get('retCode')}, retMsg: {response.get('retMsg')}")
            except Exception as e:
                error_msg = str(e)
                logger.error(
                    f"❌ Fallo al colocar orden para {symbol} (intento {attempt + 1}/{max_retries}): {error_msg}")
                if attempt < max_retries - 1:
                    delay = min(2 ** attempt, max_delay)
                    logger.info(f"⏳ Reintentando en {delay} segundos...")
                    await asyncio.sleep(delay)
                else:
                    return {"success": False, "order_id": None, "message": error_msg}
            finally:
                if session:
                    session = None

        return {"success": False, "order_id": None, "message": "Máximo de reintentos alcanzado"}

    async def _check_order_status(self, symbol: str, trade: Trade, position: Position, quantity: float,
                                  side: str) -> None:
        try:
            if not trade.order_id:
                logger.error(f"❌ No se encontró order_id para el trade en {symbol}")
                return
            order_id: str = trade.order_id
            session = HTTP(testnet=config.USE_TESTNET, api_key=config.BYBIT_API_KEY, api_secret=config.BYBIT_API_SECRET)
            max_retries = 5
            check_interval = 5
            executed_qty = 0.0  # Definir executed_qty por defecto

            with SessionLocal() as db:
                # Verificar si la orden inicial (compra o venta) se ha completado
                for attempt in range(max_retries):
                    timestamp = await get_synced_time()
                    response = session.get_open_orders(category="spot", symbol=symbol, orderId=order_id,
                                                       timestamp=str(timestamp), recvWindow=str(config.RECV_WINDOW))
                    if response.get("retCode") == 0 and not response["result"]["list"]:
                        history_response = session.get_order_history(category="spot", symbol=symbol, orderId=order_id,
                                                                     timestamp=str(timestamp),
                                                                     recvWindow=str(config.RECV_WINDOW))
                        if history_response.get("retCode") == 0 and history_response["result"]["list"]:
                            order_info = history_response["result"]["list"][0]
                            order_status = order_info.get("orderStatus", "").lower()
                            executed_qty = float(order_info.get("cumExecQty", "0.0"))
                            close_price = float(order_info.get("avgPrice", trade.entry_price))

                            if order_status in ["filled", "partiallyfilled"]:
                                if executed_qty < quantity:
                                    quantity = executed_qty
                                logger.info(
                                    f"✅ Orden {order_id} ejecutada: {order_status}, Cantidad ejecutada: {executed_qty}")
                                break
                            elif order_status in ["cancelled", "rejected"]:
                                trade.decision = "cancelled"
                                position.status = "cancelled"
                                db.commit()
                                self.positions[symbol] = [pos for pos in self.positions[symbol] if
                                                          pos["order_id"] != order_id]
                                logger.info(f"✅ Orden {order_id} cancelada")
                                await self._save_results(symbol, trade)
                                return
                        else:
                            logger.warning(
                                f"⚠ Intento {attempt + 1}/{max_retries} fallido al verificar estado de la orden {order_id}: {history_response.get('retMsg', 'Respuesta vacía')}")
                    else:
                        open_order_response = session.get_open_orders(
                            category="spot",
                            symbol=symbol,
                            orderId=order_id,
                            timestamp=str(timestamp),
                            recvWindow=str(config.RECV_WINDOW)
                        )
                        if open_order_response.get("retCode") == 0 and open_order_response["result"]["list"]:
                            order_info = open_order_response["result"]["list"][0]
                            order_status = order_info.get("orderStatus", "").lower()
                            executed_qty = float(order_info.get("cumExecQty", "0.0"))
                            close_price = float(order_info.get("avgPrice", trade.entry_price))
                            if order_status in ["filled", "partiallyfilled"]:
                                if executed_qty < quantity:
                                    quantity = executed_qty
                                logger.info(
                                    f"✅ Orden {order_id} ejecutada (vía get_open_orders): {order_status}, Cantidad ejecutada: {executed_qty}")
                                break
                            elif order_status in ["cancelled", "rejected"]:
                                trade.decision = "cancelled"
                                position.status = "cancelled"
                                db.commit()
                                self.positions[symbol] = [pos for pos in self.positions[symbol] if
                                                          pos["order_id"] != order_id]
                                logger.info(f"✅ Orden {order_id} cancelada")
                                await self._save_results(symbol, trade)
                                return
                        logger.warning(
                            f"⚠ Intento {attempt + 1}/{max_retries} fallido al verificar estado de la orden {order_id}: {response.get('retMsg', 'Respuesta vacía')}")
                    await asyncio.sleep(check_interval)

                if executed_qty == 0.0:
                    logger.error(f"❌ Orden {order_id} no se ejecutó, intentando cerrar manualmente")
                    # Cancelar la orden y cerrar manualmente
                    timestamp = await get_synced_time()
                    cancel_response = session.cancel_order(
                        category="spot",
                        symbol=symbol,
                        orderId=order_id,
                        timestamp=str(timestamp),
                        recvWindow=str(config.RECV_WINDOW)
                    )
                    if cancel_response.get("retCode") == 0:
                        logger.info(f"✅ Orden {order_id} cancelada manualmente")
                    else:
                        logger.error(f"❌ Fallo al cancelar orden {order_id}: {cancel_response.get('retMsg')}")

                    # Colocar una nueva orden al precio actual para cerrar la posición
                    last_price = self.ticker_data.get(symbol, {"last_price": 0.0})["last_price"]
                    close_side = "sell" if side == "buy" else "buy"
                    close_result = await self._place_order(symbol, close_side, quantity, last_price, None, None)
                    if close_result.get("success", False):
                        close_price = last_price
                        profit_loss = (close_price - trade.entry_price) * quantity * (
                                1 - config.FEE_RATE) if side == "buy" else (
                                                                                   trade.entry_price - close_price) * quantity * (
                                                                                   1 - config.FEE_RATE)
                        trade.exit_price = close_price
                        trade.profit_loss = profit_loss
                        trade.decision = "closed (manual)"
                        position.status = "closed"
                        db.commit()

                        self.positions[symbol] = [pos for pos in self.positions[symbol] if pos["order_id"] != order_id]
                        self.capital += profit_loss
                        logger.info(
                            f"✅ Posición cerrada manualmente para {symbol}: P&L={profit_loss:.2f}, Capital={self.capital:.2f}")
                        await self._save_results(symbol, trade)
                    else:
                        logger.error(
                            f"❌ Fallo al cerrar posición manualmente para {symbol}: {close_result.get('message')}")
                    return

                # Monitorear TP/SL y métricas para cerrar la posición
                while position.status == "open" and self.running:
                    # Obtener el precio actual
                    ticker_url = f"{BASE_URL}/v5/market/tickers"
                    params = {"category": "spot", "symbol": symbol}
                    async with aiohttp.ClientSession() as session:
                        ticker_response = await fetch_data(session, ticker_url, params)
                        if ticker_response and "result" in ticker_response and "list" in ticker_response["result"]:
                            current_price = float(ticker_response["result"]["list"][0]["lastPrice"])
                        else:
                            logger.warning(
                                f"⚠ No se pudo obtener el precio actual para {symbol}, usando último conocido")
                            current_price = self.ticker_data[symbol]["last_price"]

                    # Verificar TP y SL
                    if side == "buy":
                        if current_price >= position.tp:
                            logger.info(f"📈 Take Profit alcanzado para {symbol}: {current_price} >= {position.tp}")
                            close_reason = "TP"
                        elif current_price <= position.sl:
                            logger.info(f"📉 Stop Loss alcanzado para {symbol}: {current_price} <= {position.sl}")
                            close_reason = "SL"
                        else:
                            close_reason = None
                    else:
                        if current_price <= position.tp:
                            logger.info(f"📈 Take Profit alcanzado para {symbol}: {current_price} <= {position.tp}")
                            close_reason = "TP"
                        elif current_price >= position.sl:
                            logger.info(f"📉 Stop Loss alcanzado para {symbol}: {current_price} >= {position.sl}")
                            close_reason = "SL"
                        else:
                            close_reason = None

                    # Verificar métricas para cerrar
                    if not close_reason:
                        candles = self.candles.get(symbol, [])
                        candle_data = [
                            {"open": c["open"], "high": c["high"], "low": c["low"], "close": c["close"],
                             "volume": c["volume"]}
                            for c in candles
                        ]
                        orderbook = self.orderbook_data.get(symbol, {"bids": [], "asks": []})
                        ticker = self.ticker_data.get(symbol, {"last_price": current_price})
                        metrics = utils.calculate_metrics(candle_data, orderbook, ticker)
                        decision = self._determine_decision(symbol, metrics, quantity)
                        if decision == "sell" and side == "buy":
                            logger.info(
                                f"📉 Señal de venta detectada para cerrar posición en {symbol}: Combined={metrics.get('combined', 0.0):.4f}")
                            close_reason = "Metrics"
                        elif decision == "buy" and side == "sell":
                            logger.info(
                                f"📈 Señal de compra detectada para cerrar posición en {symbol}: Combined={metrics.get('combined', 0.0):.4f}")
                            close_reason = "Metrics"

                    if close_reason:
                        # Colocar orden al precio actual para cerrar la posición
                        close_side = "sell" if side == "buy" else "buy"
                        close_result = await self._place_order(symbol, close_side, quantity, current_price, None, None)
                        if close_result.get("success", False):
                            try:
                                close_price = current_price
                                # Cálculo detallado de comisiones
                                entry_fee = trade.entry_price * quantity * config.FEE_RATE
                                exit_fee = close_price * quantity * config.FEE_RATE
                                total_fees = entry_fee + exit_fee

                                # Cálculo de P&L bruto y neto
                                raw_profit_loss = (close_price - trade.entry_price) * quantity if side == "buy" else \
                                    (trade.entry_price - close_price) * quantity
                                profit_loss = raw_profit_loss - total_fees

                                # Actualizar estado con manejo de transacción
                                trade.exit_price = close_price
                                trade.profit_loss = profit_loss
                                trade.decision = f"closed ({close_reason})"
                                position.status = TradeStatus.CLOSED.value
                                db.commit()
                                logger.info(f"✅ Estado actualizado correctamente para trade {trade.trade_id}")
                            except Exception as e:
                                db.rollback()
                                logger.error(f"❌ Error al actualizar estado del trade: {e}")
                                position.status = TradeStatus.RECOVERING.value
                                db.commit()

                            self.positions[symbol] = [pos for pos in self.positions[symbol] if
                                                      pos["order_id"] != order_id]
                            self.capital += profit_loss

                            logger.info(f"💸 Comisiones totales: {total_fees:.4f} USDT")
                            logger.info(f"📊 P&L bruto: {raw_profit_loss:.4f} USDT, P&L neto: {profit_loss:.4f} USDT")
                            logger.info(
                                f"✅ Posición cerrada para {symbol}: P&L={profit_loss:.2f}, Capital={self.capital:.2f}, Motivo={close_reason}")
                            await self._save_results(symbol, trade)
                        else:
                            logger.error(f"❌ Fallo al cerrar posición para {symbol}: {close_result.get('message')}")
                        return

                    await asyncio.sleep(check_interval)
        except Exception as e:
            logger.error(f"❌ Error en _check_order_status para {symbol}, OrderID={order_id}: {e}")
            self.error_count += 1
            self.consecutive_errors += 1
            if self.consecutive_errors >= self.max_consecutive_errors:
                logger.warning(
                    f"⏸ Pausando bot por {self.pause_duration} segundos tras {self.consecutive_errors} errores.")
                self.paused = True
                await asyncio.sleep(self.pause_duration)
                self.paused = False
                self.consecutive_errors = 0
            if self.error_count >= self.max_errors:
                logger.critical(f"❌ Demasiados errores ({self.error_count}). Deteniendo bot.")
                self.running = False

    async def _save_results(self, symbol: str, trade_result: Optional[Trade] = None) -> None:
        try:
            with SessionLocal() as db:
                # Obtener todos los trades de la base de datos
                trades_by_symbol = {sym: [] for sym in self.symbols}
                for sym in self.symbols:
                    trades = db.query(Trade).filter_by(symbol=sym).order_by(Trade.timestamp.desc()).all()
                    trades_by_symbol[sym] = [{
                        "trade_id": t.trade_id,
                        "timestamp": t.timestamp.isoformat(),
                        "symbol": t.symbol,
                        "action": t.action,
                        "entry_price": t.entry_price,
                        "exit_price": t.exit_price,
                        "quantity": t.quantity,
                        "profit_loss": t.profit_loss if t.profit_loss is not None else 0.0,
                        "decision": t.decision,
                        "combined": t.combined,
                        "ild": t.ild,
                        "egm": t.egm,
                        "rol": t.rol,
                        "pio": t.pio,
                        "ogm": t.ogm,
                        "risk_reward_ratio": t.risk_reward_ratio,
                        "status": "closed" if t.exit_price is not None else "open",
                        "order_id": t.order_id
                    } for t in trades]

                total_profit = sum(
                    trade["profit_loss"] for sym in self.symbols for trade in trades_by_symbol[sym]
                    if trade["profit_loss"] > 0 and trade["status"] == "closed"
                )
                total_loss = sum(
                    trade["profit_loss"] for sym in self.symbols for trade in trades_by_symbol[sym]
                    if trade["profit_loss"] < 0 and trade["status"] == "closed"
                )
                total_trades = sum(
                    1 for sym in self.symbols for trade in trades_by_symbol[sym] if trade["status"] == "closed"
                )
                profit_by_symbol = {
                    sym: round(
                        sum(trade["profit_loss"] for trade in trades_by_symbol[sym]
                            if trade["profit_loss"] > 0 and trade["status"] == "closed"), 2
                    ) for sym in self.symbols
                }
                loss_by_symbol = {
                    sym: round(
                        sum(trade["profit_loss"] for trade in trades_by_symbol[sym]
                            if trade["profit_loss"] < 0 and trade["status"] == "closed"), 2
                    ) for sym in self.symbols
                }

                net_profit = total_profit + total_loss
                win_rate = (len([trade for sym in self.symbols for trade in trades_by_symbol[sym]
                                 if trade["profit_loss"] > 0 and trade["status"] == "closed"]) / total_trades
                            ) if total_trades > 0 else 0
                avg_profit_per_trade = round(net_profit / total_trades, 4) if total_trades > 0 else 0

                # Verificar si hay cambios significativos antes de guardar
                if total_trades == self.last_saved_total_trades and abs(self.capital - self.last_saved_capital) < 0.01:
                    logger.debug("📊 No hay cambios significativos en los resultados. Saltando guardado.")
                    return

                results = {
                    "metadata": {
                        "session_start": self.session_start,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "capital_inicial": self.initial_capital,
                        "capital_actual": self.capital,
                        "capital_final": self.capital,
                        "total_pnl": round(net_profit, 2),
                        "total_trades": total_trades,
                        "iterations": self.iterations,
                        "running": self.running
                    },
                    "summary": {
                        "total_profit": round(total_profit, 2),
                        "total_loss": round(total_loss, 2),
                        "net_profit": round(net_profit, 2),
                        "win_rate": round(win_rate * 100, 2),
                        "avg_profit_per_trade": avg_profit_per_trade
                    },
                    "by_symbol": {
                        sym: {
                            "profit": profit_by_symbol[sym],
                            "loss": loss_by_symbol[sym],
                            "net_profit": round(profit_by_symbol[sym] + loss_by_symbol[sym], 2),
                            "trade_count": len(
                                [trade for trade in trades_by_symbol[sym] if trade["status"] == "closed"])
                        } for sym in self.symbols
                    },
                    "trades": trades_by_symbol
                }
                if trade_result:
                    results["metadata"]["last_trade_timestamp"] = trade_result.timestamp.isoformat()
                    results["last_trade"] = {
                        "trade_id": trade_result.trade_id,
                        "timestamp": trade_result.timestamp.isoformat(),
                        "symbol": trade_result.symbol,
                        "action": trade_result.action,
                        "entry_price": trade_result.entry_price,
                        "exit_price": trade_result.exit_price,
                        "quantity": trade_result.quantity,
                        "profit_loss": trade_result.profit_loss if trade_result.profit_loss is not None else 0.0,
                        "decision": trade_result.decision,
                        "combined": trade_result.combined,
                        "ild": trade_result.ild,
                        "egm": trade_result.egm,
                        "rol": trade_result.rol,
                        "pio": trade_result.pio,
                        "ogm": trade_result.ogm,
                        "risk_reward_ratio": trade_result.risk_reward_ratio,
                        "status": "closed" if trade_result.exit_price is not None else "open"
                    }

                log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
                utils.save_results(results, log_dir, self.session_start)
                logger.info(
                    f"📊 Resultados guardados: Total PNL={round(net_profit, 2)} USDT, Capital={self.capital:.2f} USDT")

                # Actualizar el estado guardado
                self.last_saved_total_trades = total_trades
                self.last_saved_capital = self.capital
        except Exception as e:
            logger.error(f"❌ Error al guardar resultados: {e}")

        self.trade_buffer = {symbol: [] for symbol in self.symbols}

    async def reset_trades(self) -> None:
        try:
            self.positions = {symbol: [] for symbol in self.symbols}
            self.trade_buffer = {symbol: [] for symbol in self.symbols}
            self.trade_id_counter = 1

            with SessionLocal() as db:
                db.query(Trade).delete()
                db.query(Position).delete()
                db.query(MarketData).delete()
                db.query(Orderbook).delete()
                db.query(MarketTicker).delete()
                db.commit()

            self.iterations = 0
            self.last_trade_time = {symbol: datetime.min.replace(tzinfo=timezone.utc) for symbol in self.symbols}
            self.last_kline_time = {symbol: 0 for symbol in self.symbols}
            self.error_count = 0
            self.consecutive_errors = 0

            for symbol in self.symbols:
                try:
                    session = HTTP(
                        testnet=config.USE_TESTNET,
                        api_key=config.BYBIT_API_KEY,
                        api_secret=config.BYBIT_API_SECRET
                    )
                    timestamp = await get_synced_time()
                    session.cancel_all_orders(
                        category="spot",
                        symbol=symbol,
                        timestamp=str(timestamp),
                        recvWindow=str(config.RECV_WINDOW)
                    )
                    logger.info(f"✅ Todas las órdenes abiertas para {symbol} canceladas")
                except Exception as e:
                    logger.error(f"❌ Error al cancelar órdenes para {symbol}: {e}")

            logger.info("🧹 Trades reseteados exitosamente")
        except Exception as e:
            logger.error(f"❌ Error al resetear trades: {e}")
            raise

    def stop(self) -> None:
        self.running = False
        if self.ws:
            asyncio.create_task(self.ws.close())
        # Guardar resultados finales solo si hay cambios
        if any(self.trade_buffer.values()) or self.last_saved_total_trades != sum(
                1 for sym in self.symbols for trade in self.positions.get(sym, []) if trade["status"] == "closed"
        ) or abs(self.capital - self.last_saved_capital) >= 0.01:
            asyncio.create_task(self._save_results(None, None))
        logger.info("🛑 Bot detenido.")


app = FastAPI()
bot = NertzMetalEngine()


@app.get("/settings")
async def get_settings() -> Dict[str, Dict[str, Union[str, float, Dict[str, float]]]]:
    settings = {
        symbol: {
            "symbol": symbol,
            "capital": bot.capital,
            "risk_factor": config.RISK_FACTOR,
            "min_trade_size": config.MIN_TRADE_SIZE,
            "max_trade_size": config.MAX_TRADE_SIZE,
            "metrics": await get_metrics(symbol, next(get_db()))
        } for symbol in bot.symbols
    }
    return settings


@app.get("/market_data/{symbol}")
async def get_market_data(symbol: str, db: Session = Depends(get_db)) -> Dict[
    str, Union[str, List[Dict[str, Union[str, float]]]]]:
    candles = db.query(MarketData).filter(MarketData.symbol == symbol).order_by(MarketData.timestamp.desc()).limit(
        5).all()
    return {
        "symbol": symbol,
        "candles": [{"timestamp": c.timestamp.isoformat(), "open": float(c.open), "high": float(c.high),
                     "low": float(c.low), "close": float(c.close), "volume": float(c.volume)} for c in candles],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/ticker/{symbol}")
async def get_ticker(symbol: str, db: Session = Depends(get_db)) -> Dict[str, Union[str, float]]:
    ticker = db.query(MarketTicker).filter(MarketTicker.symbol == symbol).order_by(
        MarketTicker.timestamp.desc()).first()
    return {
        "symbol": symbol,
        "last_price": ticker.last_price if ticker else 0.0,
        "volume_24h": ticker.volume_24h if ticker else 0.0,
        "high_24h": ticker.high_24h if ticker else 0.0,
        "low_24h": ticker.low_24h if ticker else 0.0,
        "timestamp": ticker.timestamp.isoformat() if ticker else datetime.now(timezone.utc).isoformat()
    }


@app.get("/metrics/{symbol}")
async def get_metrics(symbol: str, db: Session = Depends(get_db)) -> Dict[str, Union[str, Dict[str, float]]]:
    candles = db.query(MarketData).filter(MarketData.symbol == symbol).order_by(MarketData.timestamp.desc()).limit(
        5).all()
    candle_data = [{"open": float(c.open), "high": float(c.high), "low": float(c.low), "close": float(c.close),
                    "volume": float(c.volume)} for c in candles]
    orderbook = bot.orderbook_data.get(symbol, {"bids": [], "asks": []})
    ticker = bot.ticker_data.get(symbol, {"last_price": 0.0})
    metrics = utils.calculate_metrics(candle_data, orderbook, ticker)
    return {
        "symbol": symbol,
        "metrics": metrics,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.post("/config/update_thresholds")
async def update_thresholds(egm_buy_threshold: float, egm_sell_threshold: float) -> Dict[str, str]:
    config.EGM_BUY_THRESHOLD = egm_buy_threshold
    config.EGM_SELL_THRESHOLD = egm_sell_threshold
    logger.info(f"✅ Umbrales actualizados: buy={egm_buy_threshold}, sell={egm_sell_threshold}")
    return {"message": "Umbrales actualizados"}


@app.get("/orderbook/{symbol}")
async def get_orderbook(symbol: str, db: Session = Depends(get_db)) -> Dict[str, Union[str, List[List[str]]]]:
    orderbook = bot.orderbook_data.get(symbol, {"bids": [], "asks": []})
    return {
        "symbol": symbol,
        "bids": orderbook["bids"],
        "asks": orderbook["asks"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/candles/{symbol}/{limit}")
async def get_candles(symbol: str, limit: int = 5, db: Session = Depends(get_db)) -> Dict[
    str, Union[str, List[Dict[str, Union[str, float]]]]]:
    candles = db.query(MarketData).filter(MarketData.symbol == symbol).order_by(MarketData.timestamp.desc()).limit(
        limit).all()
    return {
        "symbol": symbol,
        "candles": [{"timestamp": c.timestamp.isoformat(), "open": float(c.open), "high": float(c.high),
                     "low": float(c.low), "close": float(c.close), "volume": float(c.volume)} for c in candles],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/trades/{symbol}")
async def get_trades(symbol: str, db: Session = Depends(get_db)) -> Dict[
    str, Union[str, List[Dict[str, Union[str, float, int]]]]]:
    trades = bot.positions.get(symbol, [])
    return {
        "symbol": symbol,
        "trades": trades,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.post("/execute_trade/{symbol}")
async def execute_trade(symbol: str, db: Session = Depends(get_db)) -> Dict[str, str]:
    await bot._execute_trade(symbol, db)
    return {"message": f"✅ Trade ejecutado para {symbol}", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/config")
async def get_config() -> Dict[str, Union[str, float, int, bool]]:
    return {
        "symbol": config.SYMBOL,
        "timeframe": config.TIMEFRAME,
        "order_type": config.ORDER_TYPE,
        "time_in_force": config.TIME_IN_FORCE,
        "orderbook_depth": config.ORDERBOOK_DEPTH,
        "use_testnet": config.USE_TESTNET,
        "capital_usdt": bot.capital,
        "risk_factor": config.RISK_FACTOR,
        "min_trade_size": config.MIN_TRADE_SIZE,
        "max_trade_size": config.MAX_TRADE_SIZE,
        "fee_rate": config.FEE_RATE,
        "tp_percentage": config.TP_PERCENTAGE,
        "sl_percentage": config.SL_PERCENTAGE,
        "egm_buy_threshold": config.EGM_BUY_THRESHOLD,
        "egm_sell_threshold": config.EGM_SELL_THRESHOLD,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/orders/open/{symbol}")
async def get_open_orders(symbol: str, db: Session = Depends(get_db)) -> Dict[str, Union[str, List[Dict[str, Any]]]]:
    open_positions = db.query(Position).filter(Position.symbol == symbol, Position.status == "open").all()
    return {
        "symbol": symbol,
        "open_orders": [{
            "order_id": p.order_id,
            "action": p.action,
            "entry_price": p.entry_price,
            "quantity": p.quantity,
            "tp": p.tp,
            "sl": p.sl,
            "timestamp": p.timestamp.isoformat()
        } for p in open_positions],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.post("/pause")
async def pause_bot() -> Dict[str, str]:
    if bot.running and not bot.paused:
        bot.paused = True
        logger.info("⏸ Bot pausado.")
        return {"message": "✅ Bot pausado", "timestamp": datetime.now(timezone.utc).isoformat()}
    return {"message": "⚠️ Bot ya está pausado o detenido", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/resume")
async def resume_bot() -> Dict[str, str]:
    if bot.running and bot.paused:
        bot.paused = False
        logger.info("▶️ Bot reanudado.")
        return {"message": "✅ Bot reanudado", "timestamp": datetime.now(timezone.utc).isoformat()}
    return {"message": "⚠️ Bot no está pausado o no está corriendo",
            "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/config/update_all")
async def update_all_config(config_data: Dict[str, Union[str, float, int]]) -> Dict[str, str]:
    if "capital_usdt" in config_data:
        bot.capital = float(config_data["capital_usdt"]) if float(config_data["capital_usdt"]) > 0 else bot.capital
        bot.initial_capital = bot.capital
    if "risk_factor" in config_data:
        config.RISK_FACTOR = max(0.0, min(1.0, float(config_data["risk_factor"])))
    if "egm_buy_threshold" in config_data:
        config.EGM_BUY_THRESHOLD = float(config_data["egm_buy_threshold"])
    if "egm_sell_threshold" in config_data:
        config.EGM_SELL_THRESHOLD = float(config_data["egm_sell_threshold"])
    logger.info(f"✅ Configuración actualizada: {config_data}")
    return {"message": "Configuración actualizada", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/start")
async def start_bot() -> Dict[str, str]:
    if not bot.running:
        bot.running = True
        asyncio.create_task(bot.start_async())
        return {"message": "✅ Bot iniciado", "timestamp": datetime.now(timezone.utc).isoformat()}
    return {"message": "⚠️ Bot ya está corriendo", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/stop")
async def stop_bot() -> Dict[str, str]:
    if bot.running:
        bot.stop()
        return {"message": "🛑 Bot detenido", "timestamp": datetime.now(timezone.utc).isoformat()}
    return {"message": "⚠️ Bot ya está detenido", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/status")
async def get_status() -> Dict[str, Union[bool, int, List[str], str]]:
    return {
        "running": bot.running,
        "paused": bot.paused,
        "iterations": bot.iterations,
        "symbols": bot.symbols,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/check_reset")
async def check_reset(db: Session = Depends(get_db)):
    results_file = os.path.join(os.path.dirname(__file__), '..', 'logs', 'results.json')
    try:
        with open(results_file, "r", encoding="utf-8") as f:
            results_data = json.load(f)
    except FileNotFoundError:
        results_data = {"metadata": {"total_trades": 0}, "summary": {"total_profit": 0.0}}

    trades = db.query(Trade).all()
    positions = db.query(Position).all()
    results_reset = results_data["metadata"]["total_trades"] == 0 and results_data["summary"]["total_profit"] == 0.0
    trades_reset = len(trades) == 0 and len(positions) == 0
    return {"results_reset": results_reset, "trades_reset": trades_reset}


@app.get("/health")
async def health_check() -> Dict[str, str]:
    return {"status": "healthy" if bot.running and not bot.paused else "unhealthy",
            "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/alerts")
async def get_alerts(db: Session = Depends(get_db)) -> Dict[str, Union[str, bool, float, int, List[str]]]:
    alerts = []
    available_balance = await bot.fetch_real_balance()
    open_orders = db.query(Trade).filter(Trade.decision == "buy", Trade.exit_price == None).count()

    if available_balance < 50.0:
        alerts.append(f"⚠️ Saldo bajo detectado: {available_balance:.2f} USDT. Se recomienda depositar más fondos.")

    if open_orders > config.MAX_OPEN_ORDERS:
        alerts.append(f"⚠️ Demasiadas órdenes abiertas: {open_orders}. Máximo permitido: {config.MAX_OPEN_ORDERS}.")

    if bot.consecutive_errors > 0:
        alerts.append(
            f"⚠️ Errores consecutivos detectados: {bot.consecutive_errors}. El bot podría pausarse si alcanza {bot.max_consecutive_errors}.")

    return {
        "alerts": alerts,
        "running": bot.running,
        "paused": bot.paused,
        "available_balance": available_balance,
        "open_orders": open_orders,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8080))


async def main():
    try:
        await bot.reset_trades()
        logger.info("🚀 Iniciando bot y servidor API...")
        await asyncio.gather(bot.start_async(), server.serve())
    except Exception as e:
        logger.error(f"❌ Error crítico en main(): {e}")
        await server.shutdown()
    except KeyboardInterrupt:
        logger.info("🛑 Interrupción del usuario detectada.")
        await server.shutdown()
        bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
