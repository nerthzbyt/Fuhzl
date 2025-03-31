import asyncio
import logging
from decimal import Decimal
from typing import Dict, Optional, Any, Type

from pybit.unified_trading import HTTP
from sqlalchemy.orm import Session

from scripts.models import Trade, Position
from scripts.nertz import get_synced_time
from scripts.settings import ConfigSettings
from scripts.trade_status import TradeStatus

logger = logging.getLogger("NertzMetalEngine")
config = ConfigSettings()


class TradeManager:
    def __init__(self):
        self.session = HTTP(testnet=config.USE_TESTNET,
                            api_key=config.BYBIT_API_KEY,
                            api_secret=config.BYBIT_API_SECRET)

    async def sync_positions(self, symbol: str, db: Session) -> list[Any] | list[Type[Position]]:
        """Sincroniza las posiciones abiertas con Bybit con manejo mejorado de errores y recuperación."""
        max_retries = 8  # Aumentado el número de reintentos para mayor tolerancia
        retry_count = 0
        backoff_time = 1  # Tiempo inicial de espera reducido para respuesta más rápida
        positions_to_sync = []
        last_sync_attempt = {}
        min_sync_interval = 2  # Intervalo mínimo entre sincronizaciones para la misma orden

        try:
            # Obtener posiciones que necesitan sincronización
            positions = db.query(Position).filter_by(symbol=symbol).filter(
                Position.status.in_([TradeStatus.OPEN.value, TradeStatus.SYNCHRONIZING.value,
                                     TradeStatus.RECOVERING.value, TradeStatus.RETRYING.value,
                                     TradeStatus.NEW.value, TradeStatus.PENDING.value])
            ).all()

            if not positions:
                logger.info(f"✅ No hay posiciones para sincronizar en {symbol}")
                return []

            positions_to_sync.extend(positions)
            logger.info(f"📊 Iniciando sincronización de {len(positions_to_sync)} posiciones para {symbol}")

            while retry_count < max_retries:
                try:
                    timestamp = await get_synced_time()
                    # Obtener tanto órdenes abiertas como historial reciente
                    open_orders_response = self.session.get_open_orders(
                        category="spot",
                        symbol=symbol,
                        timestamp=str(timestamp),
                        recvWindow=str(config.RECV_WINDOW)
                    )

                    history_response = self.session.get_order_history(
                        category="spot",
                        symbol=symbol,
                        timestamp=str(timestamp),
                        recvWindow=str(config.RECV_WINDOW),
                        limit=50  # Aumentado el límite para capturar más historial
                    )

                    if open_orders_response.get("retCode") != 0 or history_response.get("retCode") != 0:
                        raise Exception("Error al obtener datos de órdenes")

                    open_orders = {order["orderId"]: order for order in open_orders_response["result"]["list"]}
                    history_orders = {order["orderId"]: order for order in history_response["result"]["list"]}

                    # Procesar cada posición
                    current_time = datetime.now().timestamp()
                    for position in positions_to_sync:
                        try:
                            # Evitar sincronizaciones demasiado frecuentes para la misma orden
                            if position.order_id in last_sync_attempt and \
                               current_time - last_sync_attempt[position.order_id] < min_sync_interval:
                                continue

                            last_sync_attempt[position.order_id] = current_time

                            # Verificar primero en órdenes abiertas
                            if position.order_id in open_orders:
                                order_data = open_orders[position.order_id]
                                new_status = self._determine_order_status(order_data)

                                # Solo actualizar si el estado es diferente y válido
                                if position.status != new_status.value and new_status != TradeStatus.FAILED:
                                    position.status = new_status.value
                                    logger.info(f"✅ Orden {position.order_id} actualizada a: {new_status.name}")

                            # Si no está en órdenes abiertas, verificar historial con más detalle
                            elif position.order_id in history_orders:
                                order_data = history_orders[position.order_id]
                                if order_data.get("status") == "Filled":
                                    position.status = TradeStatus.FILLED.value
                                    logger.info(f"✅ Orden {position.order_id} completada")
                                elif order_data.get("status") == "Cancelled":
                                    # Verificar si la cancelación fue intencional o por timeout
                                    if position.status not in [TradeStatus.CANCELED.value, TradeStatus.CLOSED.value]:
                                        if current_time - float(order_data.get("createTime", 0))/1000 > config.ORDER_TIMEOUT:
                                            position.status = TradeStatus.EXPIRED.value
                                            logger.warning(f"⚠️ Orden {position.order_id} expirada por timeout")
                                        else:
                                            position.status = TradeStatus.CLOSED.value
                                            logger.info(f"✅ Orden {position.order_id} cerrada")
                            else:
                                # Si no se encuentra la orden, verificar tiempo de creación
                                if position.status == TradeStatus.NEW.value:
                                    position.status = TradeStatus.SYNCHRONIZING.value
                                    logger.info(f"🔄 Orden {position.order_id} en sincronización")
                                continue

                            db.commit()

                        except Exception as e:
                            db.rollback()
                            logger.error(f"❌ Error procesando orden {position.order_id}: {str(e)}")
                            position.status = TradeStatus.RECOVERING.value
                            db.commit()
                            continue

                    # Retornar solo las posiciones que siguen abiertas
                    return [pos for pos in positions_to_sync if pos.status in
                            [TradeStatus.OPEN.value, TradeStatus.NEW.value, TradeStatus.PARTIALLY_FILLED.value]]

                except Exception as e:
                    logger.error(f"❌ Error en sincronización (intento {retry_count + 1}/{max_retries}): {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        await asyncio.sleep(backoff_time)
                        backoff_time *= 2
                    continue

            # Si llegamos aquí después de todos los reintentos
            logger.critical(f"❌ Sincronización fallida para {symbol} después de {max_retries} intentos")
            for position in positions_to_sync:
                position.status = TradeStatus.RECOVERING.value
            db.commit()
            return []

        except Exception as e:
            logger.critical(f"❌ Error crítico en sincronización de {symbol}: {str(e)}")
            db.rollback()
            return []

    async def _check_order_history(self, symbol: str, order_id: str) -> bool:
        """Verifica si una orden existe en el historial de órdenes."""
        try:
            timestamp = await get_synced_time()
            response = self.session.get_order_history(
                category="spot",
                symbol=symbol,
                orderId=order_id,
                timestamp=str(timestamp),
                recvWindow=str(config.RECV_WINDOW)
            )
            if response.get("retCode") == 0 and response["result"]["list"]:
                order = response["result"]["list"][0]
                return order.get("status") == "Filled"
            return False
        except Exception as e:
            logger.error(f"❌ Error al verificar historial de orden {order_id}: {str(e)}")
            return False

    def _determine_order_status(self, order_data: dict) -> TradeStatus:
        """Determina el estado actual de una orden basado en la respuesta del exchange."""
        status_mapping = {
            "Created": TradeStatus.NEW,
            "New": TradeStatus.NEW,
            "PartiallyFilled": TradeStatus.PARTIALLY_FILLED,
            "Filled": TradeStatus.FILLED,
            "Cancelled": TradeStatus.CANCELED,
            "Rejected": TradeStatus.REJECTED,
            "Untriggered": TradeStatus.UNTRIGGERED
        }
        return status_mapping.get(order_data.get("status"), TradeStatus.OPEN)

    async def cancel_order(self, symbol: str, order_id: str, db: Session) -> bool:
        """Cancela una orden específica con mecanismos de recuperación."""
        timestamp = await get_synced_time()
        try:
            response = self.session.cancel_order(
                category="spot",
                symbol=symbol,
                orderId=order_id,
                timestamp=str(timestamp),
                recvWindow=str(config.RECV_WINDOW)
            )

            if response.get("retCode") == 0:
                position = db.query(Position).filter_by(order_id=order_id).first()
                if position:
                    position.status = TradeStatus.CANCELLED
                    db.commit()
                    logger.info(f"✅ Orden {order_id} cancelada exitosamente.")
                return True
            else:
                logger.warning(f"⚠️ Orden {order_id} no pudo ser cancelada, marcando como RECOVERING")
                position = db.query(Position).filter_by(order_id=order_id).first()
                if position:
                    position.status = TradeStatus.RECOVERING
                    db.commit()
                return False

        except Exception as e:
            logger.error(f"❌ Error inesperado al cancelar orden {order_id}: {str(e)}")
            position = db.query(Position).filter_by(order_id=order_id).first()
            if position:
                position.status = TradeStatus.FAILED
                db.commit()
            return False

    async def place_order(self, symbol: str, action: str, quantity: float,
                          price: float, tp: Optional[float], sl: Optional[float],
                          retry_count: int = 0) -> Dict:
        """Coloca una nueva orden en el mercado con reintentos y validaciones."""
        timestamp = await get_synced_time()
        try:
            order_params = {
                "category": "spot",
                "symbol": symbol,
                "side": action.upper(),
                "orderType": config.ORDER_TYPE,
                "qty": str(quantity),
                "price": str(price),
                "timeInForce": config.TIME_IN_FORCE,
                "timestamp": str(timestamp),
                "recvWindow": str(config.RECV_WINDOW)
            }

            if tp is not None:
                order_params["takeProfit"] = str(tp)
            if sl is not None:
                order_params["stopLoss"] = str(sl)

            # Validación de cantidad mínima
            if float(quantity) < config.MIN_TRADE_SIZE:
                logger.error(f"❌ Cantidad {quantity} menor al mínimo permitido {config.MIN_TRADE_SIZE}")
                return {"success": False, "message": "Cantidad menor al mínimo permitido"}

            response = self.session.place_order(**order_params)

            if response.get("retCode") == 0:
                order_id = response["result"]["orderId"]
                logger.info(f"✅ Orden colocada exitosamente: {order_id}")
                return {"success": True, "order_id": order_id}
            elif retry_count < config.MAX_ORDER_RETRIES:
                logger.warning(f"⚠️ Reintentando orden ({retry_count + 1}/{config.MAX_ORDER_RETRIES})")
                return await self.place_order(symbol, action, quantity, price, tp, sl, retry_count + 1)
            else:
                logger.error(
                    f"❌ Error al colocar orden después de {config.MAX_ORDER_RETRIES} intentos: {response.get('retMsg')}")
                return {"success": False, "message": response.get("retMsg")}

        except Exception as e:
            if retry_count < config.MAX_ORDER_RETRIES:
                logger.warning(
                    f"⚠️ Reintentando orden por error ({retry_count + 1}/{config.MAX_ORDER_RETRIES}): {str(e)}")
                return await self.place_order(symbol, action, quantity, price, tp, sl, retry_count + 1)
            logger.error(f"❌ Error inesperado al colocar orden: {str(e)}")
            return {"success": False, "message": str(e)}

    def update_trade_status(self, trade: Trade, exit_price: float,
                            profit_loss: float, decision: str, db: Session) -> None:
        """Actualiza el estado de un trade."""
        trade.exit_price = Decimal(str(exit_price))
        trade.profit_loss = Decimal(str(profit_loss))
        trade.decision = decision
        trade.status = TradeStatus.CLOSED
        db.commit()
        logger.info(f"✅ Trade {trade.trade_id} actualizado: P&L={profit_loss:.2f}")
