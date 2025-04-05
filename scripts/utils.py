import hashlib
import hmac
import logging
from datetime import datetime, timezone
from typing import Tuple, Dict, List, Union

import numpy as np

logger = logging.getLogger("NertzUtils")


def generate_signature(secret: str, timestamp: str, api_key: str, recv_window: str) -> str:
    string_to_sign = f"{timestamp}{api_key}{recv_window}"
    logger.debug(f"String to sign: {string_to_sign}")
    return hmac.new(secret.encode('utf-8'), string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()


def calculate_metrics(candle_data: List[Dict[str, float]], orderbook_data: Dict[str, List[List[str]]],
                      ticker_data: Dict[str, float], depth: int = 5) -> Dict[str, float]:
    # Validación inicial
    if not all([candle_data, len(candle_data) >= 2, orderbook_data.get("bids"), orderbook_data.get("asks"),
                ticker_data.get('last_price')]):
        logger.warning("Datos insuficientes para métricas, usando valores mínimos")
        return {
            "combined": 0.0, "ild": 0.0, "egm": 0.0, "rol": 0.0, "pio": 0.0, "ogm": 0.0, "volatility": 0.01
        }

    try:
        last_price = float(ticker_data["last_price"])
        if last_price <= 0:
            logger.warning("Precio inválido en ticker_data, usando valor predeterminado: 75000.0")
            last_price = 75000.0

        # Preparación de datos con manejo de valores faltantes
        closes = np.array([float(c["close"]) for c in candle_data[-5:]], dtype=np.float64)
        highs = np.array([float(c.get("high", last_price)) for c in candle_data[-5:]], dtype=np.float64)
        lows = np.array([float(c.get("low", last_price)) for c in candle_data[-5:]], dtype=np.float64)
        volumes = np.array([float(c.get("volume", 0)) for c in candle_data[-5:]], dtype=np.float64)

        if len(highs) == 0 or len(lows) == 0:
            logger.warning("Datos insuficientes (highs/lows vacíos), usando valores mínimos")
            return {
                "combined": 0.0, "ild": 0.0, "egm": 0.0, "rol": 0.0, "pio": 0.0, "ogm": 0.0, "volatility": 0.01
            }

        avg_price = float(np.mean(closes))
        price_range = float(max(highs.max() - lows.min(), 1e-8))
        volatility = (highs.max() - lows.min()) / last_price if highs.max() > lows.min() else 0.01

        # Cálculo de métricas del orderbook
        bids = orderbook_data["bids"][:depth]
        asks = orderbook_data["asks"][:depth]
        bid_volume = sum(float(bid[1]) for bid in bids if float(bid[1]) > 0)
        ask_volume = sum(float(ask[1]) for ask in asks if float(ask[1]) > 0)
        total_volume = bid_volume + ask_volume + 1e-6
        ild = np.clip((bid_volume - ask_volume) / total_volume, -1.0, 1.0)

        egm = np.clip((last_price - avg_price) / price_range, -1.0, 1.0)

        bid_value = sum(float(bid[0]) * float(bid[1]) for bid in bids if float(bid[1]) > 0)
        ask_value = sum(float(ask[0]) * float(ask[1]) for ask in asks if float(ask[1]) > 0)
        total_value = bid_value + ask_value + 1e-6
        rol = np.clip((bid_value - ask_value) / total_value, -1.0, 1.0)

        pio = 0.0
        if len(volumes) >= 2:
            avg_volume = float(np.mean(volumes[1:])) + 1e-6
            pio = np.clip((volumes[0] - avg_volume) / avg_volume, -1.0, 1.0)

        best_bid = float(bids[0][0]) if bids else last_price
        best_ask = float(asks[0][0]) if asks else last_price
        spread = (best_ask - best_bid) / last_price if last_price > 0 else 0.01
        ogm = 1.0 - np.clip(spread / 0.015, 0, 1.0)  # Ajustado para ser más conservador con el spread

        combined = np.clip((0.3 * egm + 0.35 * ild + 0.15 * rol + 0.1 * pio + 0.1 * ogm) * 10, -10.0,
                           10.0)  # Ajustado para dar más peso a ILD y menos a EGM

        logger.debug(f"Métricas calculadas para {candle_data[0].get('symbol', 'unknown')}: "
                     f"combined={combined:.4f}, ild={ild:.4f}, egm={egm:.4f}, rol={rol:.4f}, "
                     f"pio={pio:.4f}, ogm={ogm:.4f}, volatility={volatility:.4f}")
        return {
            "combined": combined, "ild": ild, "egm": egm, "rol": rol, "pio": pio, "ogm": ogm, "volatility": volatility
        }
    except Exception as e:
        logger.error(f"Error en calculate_metrics: {str(e)}, Tipo: {type(e).__name__}", exc_info=True)
        return {
            "combined": 0.0, "ild": 0.0, "egm": 0.0, "rol": 0.0, "pio": 0.0, "ogm": 0.0, "volatility": 0.01
        }


def save_results(results: dict, log_dir: str, session_start: str):
    from scripts.unified_logger import UnifiedLogger
    unified_logger = UnifiedLogger(log_dir)
    unified_logger.start_session(session_start)
    unified_logger.update_session_data(results)
    logger.info(f"📄 Resultados guardados en el registro unificado")


def timestamp_to_datetime(timestamp: Union[int, str]) -> datetime:
    if isinstance(timestamp, str):
        timestamp = int(timestamp)
    return datetime.fromtimestamp(timestamp // 1000, tz=timezone.utc)


def calculate_tp_sl(price: float, volatility: float, action: str, tp_factor: float = 1.5, sl_factor: float = 1.0) -> \
        Tuple[float, float]:
    try:
        volatility = max(volatility, 0.01)  # Asegurar un valor mínimo
        price_range = volatility * price
        if action.lower() == "buy":
            tp = price + (price_range * tp_factor * 1.2)  # Aumentado para asegurar ganancias
            sl = price - (price_range * sl_factor * 0.8)  # Reducido para proteger capital
        else:
            tp = price - (price_range * tp_factor * 1.2)
            sl = price + (price_range * sl_factor * 0.8)
        tp = max(tp, 0.01)  # Asegurar que no sea 0
        sl = max(sl, 0.01)
        return round(tp, 2), round(sl, 2)
    except Exception as e:
        logger.error(f"❌ Error en calculate_tp_sl: {e}")
        # Valores predeterminados en caso de error
        if action.lower() == "buy":
            tp = price * 1.02  # 2% por encima
            sl = price * 0.98  # 2% por debajo
        else:
            tp = price * 0.98  # 2% por debajo
            sl = price * 1.02  # 2% por encima
        return round(tp, 2), round(sl, 2)


class BaseTradingStrategy:
    def __init__(self, connector=None, data_manager=None, **kwargs):
        if kwargs:
            self.logger.debug(f"Unused kwargs: {kwargs}")
        self.logger = logging.getLogger("NertzMetalEngine")
        self.connector = connector
        self.data_manager = data_manager


def evaluate_trend(short_ema: float, mid_ema: float, long_ema: float) -> Tuple[str, str]:
    if short_ema > mid_ema > long_ema:
        return "BUY", "Cruzamiento alcista de Triple EMA"
    elif short_ema < mid_ema < long_ema:
        return "SELL", "Cruzamiento bajista de Triple EMA"
    return "HOLD", "No hay confirmación clara"


class TpslStrategy(BaseTradingStrategy):
    def __init__(self, connector=None, data_manager=None, short_window: int = 5, mid_window: int = 10,
                 long_window: int = 20, tp_percentage: float = 1.5, sl_percentage: float = 0.5,
                 combined_buy_threshold: float = 2.0, combined_sell_threshold: float = -2.0, fee_rate: float = 0.002,
                 max_drawdown: float = 0.05, **kwargs):
        super().__init__(connector, data_manager, **kwargs)
        self.short_window = short_window
        self.mid_window = mid_window
        self.long_window = long_window
        self.tp_percentage = tp_percentage
        self.sl_percentage = sl_percentage
        self.combined_buy_threshold = combined_buy_threshold
        self.combined_sell_threshold = combined_sell_threshold
        self.fee_rate = fee_rate
        self.max_drawdown = max_drawdown

    def calculate_ema(self, prices: List[float], window: int) -> float:
        try:
            if not prices or len(prices) < 1:
                self.logger.warning(f"No hay precios para EMA con ventana {window}")
                return 0.0
            if len(prices) < window:
                self.logger.debug(f"Pocos datos ({len(prices)}), usando promedio simple")
                return sum(prices) / len(prices)
            alpha = 2 / (window + 1)
            ema = prices[-window]
            for price in prices[-window + 1:]:
                ema = (price * alpha) + (ema * (1 - alpha))
            return ema
        except Exception as e:
            self.logger.error(f"Error en calculate_ema: {e}")
            return 0.0

    def generate_signal(self, market_data: Dict[str, List[float]], metrics: Dict[str, float]) -> Dict[str, any]:
        if not market_data.get("close_prices"):
            self.logger.warning("Faltan 'close_prices' en market_data")
            return {"action": "HOLD", "confidence": 0.0, "take_profit": 0.0, "stop_loss": 0.0, "reason": "Sin datos"}

        closing_prices = market_data["close_prices"]
        if len(closing_prices) < self.long_window:
            self.logger.warning(f"Datos insuficientes: {len(closing_prices)} < {self.long_window}")
            return {"action": "HOLD", "confidence": 0.0, "take_profit": 0.0, "stop_loss": 0.0,
                    "reason": "Datos insuficientes"}

        try:
            short_ema = self.calculate_ema(closing_prices, self.short_window)
            mid_ema = self.calculate_ema(closing_prices, self.mid_window)
            long_ema = self.calculate_ema(closing_prices, self.long_window)

            if short_ema == 0.0 or mid_ema == 0.0 or long_ema == 0.0:
                self.logger.warning("EMA inválida, posible error de datos")
                return {"action": "HOLD", "confidence": 0.0, "take_profit": 0.0, "stop_loss": 0.0,
                        "reason": "EMA fallida"}

            latest_price = closing_prices[-1]
            action, reason = evaluate_trend(short_ema, mid_ema, long_ema)

            combined = metrics.get("combined", 0.0)
            if action == "SELL" and combined > self.combined_sell_threshold:
                action = "HOLD"
                reason = f"Venta suspendida, combined={combined:.2f} > {self.combined_sell_threshold}"
            elif action == "BUY" and combined < self.combined_buy_threshold:
                action = "HOLD"
                reason = f"Compra suspendida, combined={combined:.2f} < {self.combined_buy_threshold}"

            take_profit, stop_loss = 0.0, 0.0
            if action in ["BUY", "SELL"]:
                take_profit, stop_loss = self.calculate_take_profit_stop_loss(
                    latest_price, action, metrics.get("volatility", 0.0),
                    market_data.get("best_ask", latest_price), market_data.get("best_bid", latest_price)
                )

            return {
                "action": action,
                "confidence": 0.9 if action in ["BUY", "SELL"] else 0.5,
                "take_profit": take_profit,
                "stop_loss": stop_loss,
                "reason": reason,
                "metrics": metrics
            }
        except Exception as e:
            self.logger.error(f"Error en generate_signal: {e}")
            return {"action": "HOLD", "confidence": 0.0, "take_profit": 0.0, "stop_loss": 0.0,
                    "reason": f"Excepción: {e}"}

    def calculate_take_profit_stop_loss(self, latest_price: float, action: str, volatility: float, best_ask: float,
                                        best_bid: float) -> Tuple[float, float]:
        try:
            if action.upper() not in ["BUY", "SELL"]:
                raise ValueError(f"Acción inválida: {action}")
            volatility = max(volatility, 0.01)
            spread = (best_ask - best_bid) / latest_price if latest_price > 0 else 0.002
            effective_tp_percentage = self.tp_percentage - (self.fee_rate * 2) - spread
            effective_sl_percentage = self.sl_percentage + (self.fee_rate * 2) + spread
            max_sl_percentage = self.max_drawdown * 75  # Reducido para limitar pérdidas
            effective_sl_percentage = min(effective_sl_percentage, max_sl_percentage)

            volatility_factor = volatility * 2.0  # Aumentado para ser más conservador
            if action.upper() == "BUY":
                take_profit = latest_price * (1 + (effective_tp_percentage + volatility_factor) / 100)
                stop_loss = latest_price * (1 - (effective_sl_percentage + volatility_factor) / 100)
            else:
                take_profit = latest_price * (1 - (effective_tp_percentage + volatility_factor) / 100)
                stop_loss = latest_price * (1 + (effective_sl_percentage + volatility_factor) / 100)

            take_profit = max(take_profit, 0.01)
            stop_loss = max(stop_loss, 0.01)
            return round(take_profit, 2), round(stop_loss, 2)
        except Exception as e:
            self.logger.error(f"Error en calculate_take_profit_stop_loss: {e}")
            if action.upper() == "BUY":
                take_profit = latest_price * (1 + self.tp_percentage / 100)
                stop_loss = latest_price * (1 - self.sl_percentage / 100)
            else:
                take_profit = latest_price * (1 - self.tp_percentage / 100)
                stop_loss = latest_price * (1 + self.sl_percentage / 100)
            return round(take_profit, 2), round(stop_loss, 2)
