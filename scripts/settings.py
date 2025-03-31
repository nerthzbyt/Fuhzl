import logging
import os
from typing import Any, Dict, Union

from dotenv import load_dotenv

logger = logging.getLogger("NertzMetalEngine")
load_dotenv()


class ConfigSettings:
    DEFAULTS = {
        # Parámetros de Capital y Riesgo
        "CAPITAL_USDT": 980.45,  # Capital inicial en mainnet
        "RISK_FACTOR": 0.002,  # Reducido a 0.2% por operación para mayor seguridad en mainnet
        "MIN_TRADE_SIZE": 0.00008,  # Ajustado para operar con menor capital en mainnet
        "MAX_TRADE_SIZE": 0.00015,  # Limitado para proteger el capital en mainnet
        "FEE_RATE": 0.001,  # Tasa de comisión estándar de Bybit

        # Parámetros de Take Profit y Stop Loss
        "TP_PERCENTAGE": 0.015,  # 1.5% para asegurar ganancias en mainnet
        "SL_PERCENTAGE": 0.005,  # 0.5% para protección más estricta en mainnet

        # Parámetros de Control
        "MAX_ITERATIONS": 1500,
        "DEFAULT_SLEEP_TIME": 3,  # Más tiempo entre iteraciones para mainnet
        "VOLUME_THRESHOLD": 2.0,  # Más exigente con el volumen en mainnet
        "PRICE_SHIFT_FACTOR": 0.001,  # Más preciso para mainnet

        # Indicadores Técnicos
        "RSI_UPPER_THRESHOLD": 70.0,  # Más conservador para ventas
        "RSI_LOWER_THRESHOLD": 30.0,  # Más conservador para compras
        "RATE_LIMIT_DELAY": 100,  # Mayor delay para evitar límites

        # Umbrales de Señales
        "PIO_THRESHOLD": 0.01,  # Más conservador
        "EGM_BUY_THRESHOLD": 0.05,  # Más estricto para compras
        "EGM_SELL_THRESHOLD": -0.03,  # Más sensible para ventas

        # Parámetros de Órdenes
        "RECV_WINDOW": 10000,
        "MAX_OPEN_ORDERS": 3,  # Reducido para mejor control

        # Configuraciones de Trading
        "VALID_SYMBOLS": ["BTCUSDT"],
        "VALID_TIMEFRAMES": ["1m", "5m", "15m", "1h"],  # Incluye 1m
        "VALID_ORDER_TYPES": ["limit", "market"],
        "VALID_TIME_IN_FORCE": ["GoodTillCancel", "ImmediateOrCancel"],
        "VALID_ORDERBOOK_DEPTHS": [5, 10, 25],
        "DEFAULT_ORDERBOOK_DEPTH": 10,  # Mayor profundidad
        "MAX_DRAWDOWN": 0.10,  # 10% máximo drawdown para mainnet
        "STRATEGY": "base",  # Estrategia predeterminada

        # Nuevos parámetros de validación
        "MAX_SPREAD": 0.001,  # Spread máximo permitido (0.1%) para mainnet
        "MIN_MARKET_DEPTH": 100.0,  # Profundidad mínima del mercado en USDT para mainnet
        "TARGET_MARKET_DEPTH": 200.0,  # Profundidad objetivo del mercado para mainnet
        "MIN_24H_VOLUME": 2000.0,  # Volumen mínimo en 24h en USDT para mainnet
        "TARGET_SPREAD": 0.0005,  # Spread objetivo (0.05%) para mainnet
        "MAX_VOLATILITY": 0.03,  # Volatilidad máxima permitida (3%) para mainnet
        "TRADE_COOLDOWN": 600,  # Tiempo base de espera entre operaciones para mainnet (segundos)
        "MAX_ORDER_RETRIES": 2  # Máximo número de reintentos para órdenes en mainnet
    }

    def __init__(self):
        self.INITIAL_BACKOFF_TIME = self._get_float("INITIAL_BACKOFF_TIME", 1.0, positive=True)
        self.RECV_WINDOW = self._get_float("RECV_WINDOW", self.DEFAULTS["RECV_WINDOW"], positive=True)
        self.BYBIT_API_KEY = self._get_env("BYBIT_API_KEY")
        self.BYBIT_API_SECRET = self._get_env("BYBIT_API_SECRET")
        self.USE_TESTNET = self._get_env_bool("USE_TESTNET", default=True)
        self.SYMBOL = self._get_validated("SYMBOL", "BTCUSDT", self.DEFAULTS["VALID_SYMBOLS"])
        self.TIMEFRAME = self._get_validated("TIMEFRAME", "1m", self.DEFAULTS["VALID_TIMEFRAMES"])
        self.ORDER_TYPE = self._get_validated("ORDER_TYPE", "limit", self.DEFAULTS["VALID_ORDER_TYPES"])
        self.TIME_IN_FORCE = self._get_validated("TIME_IN_FORCE", "GoodTillCancel",
                                                 self.DEFAULTS["VALID_TIME_IN_FORCE"])
        self.ORDERBOOK_DEPTH = self._get_validated("ORDERBOOK_DEPTH", self.DEFAULTS["DEFAULT_ORDERBOOK_DEPTH"],
                                                   self.DEFAULTS["VALID_ORDERBOOK_DEPTHS"], cast_to=int)
        self.CAPITAL_USDT = self._get_float("CAPITAL_USDT", self.DEFAULTS["CAPITAL_USDT"], positive=True)
        self.FEE_RATE = self._get_float("FEE_RATE", self.DEFAULTS["FEE_RATE"], min_value=0.0, max_value=0.1)
        self.RISK_FACTOR = self._get_float("RISK_FACTOR", self.DEFAULTS["RISK_FACTOR"], min_value=0.0, max_value=0.1)
        self.TP_PERCENTAGE = self._get_float("TP_PERCENTAGE", self.DEFAULTS["TP_PERCENTAGE"], positive=True)
        self.SL_PERCENTAGE = self._get_float("SL_PERCENTAGE", self.DEFAULTS["SL_PERCENTAGE"], positive=True)
        self.MIN_TRADE_SIZE = self._get_float("MIN_TRADE_SIZE", self.DEFAULTS["MIN_TRADE_SIZE"], positive=True)
        self.MAX_TRADE_SIZE = self._get_float("MAX_TRADE_SIZE", self.DEFAULTS["MAX_TRADE_SIZE"], positive=True)
        self.MAX_ITERATIONS = self._get_float("MAX_ITERATIONS", self.DEFAULTS["MAX_ITERATIONS"])
        self.DEFAULT_SLEEP_TIME = self._get_float("DEFAULT_SLEEP_TIME", self.DEFAULTS["DEFAULT_SLEEP_TIME"],
                                                  positive=True)
        self.EGM_BUY_THRESHOLD = self._get_float("EGM_BUY_THRESHOLD", self.DEFAULTS["EGM_BUY_THRESHOLD"])
        self.EGM_SELL_THRESHOLD = self._get_float("EGM_SELL_THRESHOLD", self.DEFAULTS["EGM_SELL_THRESHOLD"])
        self.PIO_THRESHOLD = self._get_float("PIO_THRESHOLD", self.DEFAULTS["PIO_THRESHOLD"])
        self.MAX_DRAWDOWN = self._get_float("MAX_DRAWDOWN", self.DEFAULTS["MAX_DRAWDOWN"], min_value=0.0,
                                            max_value=1.0)
        self.MAX_OPEN_ORDERS = self._get_float("MAX_OPEN_ORDERS", self.DEFAULTS["MAX_OPEN_ORDERS"], positive=True)
        self.STRATEGY = self._get_validated("STRATEGY", self.DEFAULTS["STRATEGY"],
                                            ["conservative", "base", "aggressive"])

        # Inicialización de nuevos parámetros de validación
        self.MAX_SPREAD = self._get_float("MAX_SPREAD", self.DEFAULTS["MAX_SPREAD"], min_value=0.0, max_value=0.1)
        self.MIN_MARKET_DEPTH = self._get_float("MIN_MARKET_DEPTH", self.DEFAULTS["MIN_MARKET_DEPTH"], positive=True)
        self.TARGET_MARKET_DEPTH = self._get_float("TARGET_MARKET_DEPTH", self.DEFAULTS["TARGET_MARKET_DEPTH"],
                                                   positive=True)
        self.MIN_24H_VOLUME = self._get_float("MIN_24H_VOLUME", self.DEFAULTS["MIN_24H_VOLUME"], positive=True)
        self.TARGET_SPREAD = self._get_float("TARGET_SPREAD", self.DEFAULTS["TARGET_SPREAD"], min_value=0.0,
                                             max_value=0.1)
        self.MAX_VOLATILITY = self._get_float("MAX_VOLATILITY", self.DEFAULTS["MAX_VOLATILITY"], positive=True)
        self.TRADE_COOLDOWN = self._get_float("TRADE_COOLDOWN", self.DEFAULTS["TRADE_COOLDOWN"], positive=True)
        self.MAX_ORDER_RETRIES = self._get_float("MAX_ORDER_RETRIES", self.DEFAULTS["MAX_ORDER_RETRIES"], positive=True)

        if self.MAX_OPEN_ORDERS is None:
            logger.error(
                f"❌ MAX_OPEN_ORDERS no puede ser None. Usando valor predeterminado: {self.DEFAULTS['MAX_OPEN_ORDERS']}")
            self.MAX_OPEN_ORDERS = self.DEFAULTS["MAX_OPEN_ORDERS"]

        self._log_config()

    def _get_env(self, key: str, default: Any = None) -> Union[str, None]:
        return os.getenv(key, default)

    def _get_env_bool(self, key: str, default: bool = False) -> bool:
        value = self._get_env(key, str(default))
        return value.strip().lower() in ["true", "1", "yes"]

    def _get_validated(self, key: str, default: Any, valid_values: list, cast_to: type = str) -> Any:
        value = self._get_env(key, default)
        try:
            value_casted = cast_to(value)
            if value_casted in valid_values:
                return value_casted
        except (ValueError, TypeError):
            pass
        logger.warning(f"⚠ Valor no válido para '{key}': {value}. Usando predeterminado: {default}")
        return default

    def _get_float(self, key: str, default: float, min_value: float = None, max_value: float = None,
                   positive: bool = False) -> float:
        value = self._get_env(key, default)
        try:
            value = float(value)
            if positive and value <= 0:
                raise ValueError(f"El valor de '{key}' debe ser positivo: {value}")
            if min_value is not None and value < min_value:
                raise ValueError(f"El valor de '{key}' debe ser mayor o igual a {min_value}: {value}")
            if max_value is not None and value > max_value:
                raise ValueError(f"El valor de '{key}' debe ser menor o igual a {max_value}: {value}")
            return value
        except (ValueError, TypeError) as e:
            logger.warning(f"⚠ '{key}' no es válido ({value}). Usando valor predeterminado: {default}. Error: {str(e)}")
            return default

    def _log_config(self):
        if not self.USE_TESTNET:
            if not self.BYBIT_API_KEY or not self.BYBIT_API_SECRET:
                logger.critical('❌ CRÍTICO: Claves API faltantes para trading real')
                raise ValueError('Se requieren claves API válidas para el entorno real de Bybit')

            if 'test' in self.BYBIT_API_KEY.lower():
                logger.critical('❌ CRÍTICO: Clave API de testnet detectada en entorno real')
                raise ValueError('Clave API de testnet no válida para trading real')

        logger.info(
            f'✅ Configuración validada:\n- Entorno: {"TESTNET" if self.USE_TESTNET else "REAL"}\n- Símbolos activos: {self.SYMBOL}\n- Capital inicial: {self.CAPITAL_USDT} USDT')
        logger.info(f"✅ Configuración cargada: {self.to_dict()}")

    def update_config(self, key: str, value: Any) -> bool:
        if not hasattr(self, key):
            logger.error(f"❌ Configuración '{key}' no es válida.")
            return False
        try:
            setattr(self, key, value)
            logger.info(f"✅ Configuración '{key}' actualizada: {value}")
            return True
        except Exception as e:
            logger.error(f"❌ Error al actualizar configuración '{key}': {e}")
            return False

    def update_configs(self, updates: Dict[str, Any]) -> bool:
        success = True
        for key, value in updates.items():
            if not self.update_config(key, value):
                success = False
        return success

    def to_dict(self) -> Dict[str, Any]:
        return {key: getattr(self, key) for key in vars(self)}


Config = ConfigSettings()
