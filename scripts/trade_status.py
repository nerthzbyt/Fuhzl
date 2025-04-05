import logging
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Dict, Optional

# Define a mock ConfigSettings class for demonstration purposes
class ConfigSettings:
    MAX_VOLATILITY = 0.05
    MAX_SPREAD = 0.02
    TARGET_SPREAD = 0.01
    MIN_MARKET_DEPTH = 1000
    TARGET_MARKET_DEPTH = 2000
    MIN_24H_VOLUME = 50000
    MAX_OPEN_ORDERS = 10
    MAX_ORDER_RETRIES = 3
    TRADE_COOLDOWN = 60

logger = logging.getLogger("NertzMetalEngine")
config = ConfigSettings()


class TradeStatus(Enum):
    OPEN = auto()
    CLOSED = auto()
    CANCELED = auto()
    PARTIALLY_FILLED = auto()
    REJECTED = auto()
    FILLED = auto()
    NEW = auto()
    PENDING = auto()
    EXPIRED = auto()
    UNTRIGGERED = auto()
    FAILED = auto()
    RECOVERING = auto()
    ADJUSTING = auto()
    REPLACING = auto()
    SYNCHRONIZING = auto()  # Estado durante la sincronización con el exchange
    RETRYING = auto()  # Estado durante reintentos de verificación
    VERIFYING = auto()  # Estado durante verificación de orden
    TIMEOUT = auto()  # Estado cuando una orden excede el tiempo máximo

    @classmethod
    def from_string(cls, status_str: str) -> Optional["TradeStatus"]:
        status_mapping = {
            'filled': cls.FILLED,
            'partially_filled': cls.PARTIALLY_FILLED,
            'canceled': cls.CANCELED,
            'new': cls.NEW,
            'pending': cls.PENDING,
            'expired': cls.EXPIRED,
            'untriggered': cls.UNTRIGGERED,
            'failed': cls.FAILED,
            'recovering': cls.RECOVERING,
            'adjusting': cls.ADJUSTING,
            'replacing': cls.REPLACING,
            'synchronizing': cls.SYNCHRONIZING,
            'retrying': cls.RETRYING,
            'verifying': cls.VERIFYING,
            'timeout': cls.TIMEOUT
        }

        try:
            return status_mapping.get(status_str.lower()) or cls[status_str.upper()]
        except KeyError:
            logger.error(f"Estado de trade no reconocido: {status_str}")
            return None

    def __str__(self) -> str:
        return self.name.lower()


class TradeWarningSystem:
    def __init__(self):
        self.warnings = []
        self.errors = []
        self.last_warning_time = {}
        self.warning_cooldown = 300  # 5 minutos entre advertencias similares

    def check_market_conditions(self, metrics: Dict) -> bool:
        """Verifica las condiciones del mercado y genera advertencias apropiadas."""
        current_time = datetime.now(timezone.utc).timestamp()
        is_valid = True

        # Validación de volatilidad
        volatility = metrics.get('volatility', 0)
        if volatility > config.MAX_VOLATILITY:
            self._add_warning(
                'volatility',
                f'⚠️ Alta volatilidad detectada: {volatility:.4f} > {config.MAX_VOLATILITY}',
                current_time
            )
            is_valid = False

        # Validación de spread
        spread = metrics.get('spread', 0)
        if spread > config.MAX_SPREAD:
            self._add_warning(
                'spread',
                f'⚠️ Spread elevado: {spread:.4f} > {config.MAX_SPREAD}',
                current_time
            )
            is_valid = False
        elif spread > config.TARGET_SPREAD:
            self._add_warning(
                'spread',
                f'⚠️ Spread por encima del objetivo: {spread:.4f} > {config.TARGET_SPREAD}',
                current_time
            )

        # Validación de profundidad de mercado
        market_depth = metrics.get('market_depth', 0)
        if market_depth < config.MIN_MARKET_DEPTH:
            self._add_warning(
                'market_depth',
                f'⚠️ Profundidad de mercado insuficiente: {market_depth:.2f} < {config.MIN_MARKET_DEPTH}',
                current_time
            )
            is_valid = False
        elif market_depth < config.TARGET_MARKET_DEPTH:
            self._add_warning(
                'market_depth',
                f'⚠️ Profundidad de mercado por debajo del objetivo: {market_depth:.2f} < {config.TARGET_MARKET_DEPTH}',
                current_time
            )

        # Validación de volumen
        volume_24h = metrics.get('volume_24h', 0)
        if volume_24h < config.MIN_24H_VOLUME:
            self._add_warning(
                'volume',
                f'⚠️ Volumen 24h insuficiente: {volume_24h:.2f} < {config.MIN_24H_VOLUME}',
                current_time
            )
            is_valid = False

        return is_valid

    def check_trade_conditions(self, metrics: Dict) -> bool:
        """Verifica las condiciones de trading y genera advertencias apropiadas."""
        current_time = datetime.now(timezone.utc).timestamp()
        is_valid = True

        # Validación de estado de sincronización
        sync_status = metrics.get('sync_status', '')
        if sync_status == TradeStatus.SYNCHRONIZING.value:
            self._add_warning(
                'sync',
                '⚠️ Sistema en proceso de sincronización, operaciones en espera',
                current_time
            )
            return False

        # Validación de posiciones abiertas
        open_positions = metrics.get('open_positions', 0)
        if open_positions >= config.MAX_OPEN_ORDERS:
            self._add_warning(
                'positions',
                f'⚠️ Máximo de posiciones abiertas alcanzado: {open_positions} >= {config.MAX_OPEN_ORDERS}',
                current_time
            )
            is_valid = False

        # Validación de errores consecutivos
        failed_trades = metrics.get('failed_trades_count', 0)
        if failed_trades >= config.MAX_ORDER_RETRIES:
            self._add_warning(
                'failed_trades',
                f'⚠️ Demasiados errores consecutivos: {failed_trades} >= {config.MAX_ORDER_RETRIES}',
                current_time
            )
            is_valid = False

        # Validación de tiempo entre operaciones
        last_trade_time = metrics.get('last_trade_time', 0)
        cooldown_time = config.TRADE_COOLDOWN * (1 + metrics.get('volatility', 0))
        if last_trade_time + cooldown_time > current_time:
            remaining_time = int(last_trade_time + cooldown_time - current_time)
            self._add_warning(
                'cooldown',
                f'⚠️ En período de espera: {remaining_time}s restantes',
                current_time
            )
            is_valid = False

        return is_valid

    def _add_warning(self, warning_type: str, message: str, current_time: float) -> None:
        """Añade una advertencia si ha pasado suficiente tiempo desde la última similar."""
        if warning_type not in self.last_warning_time or \
                current_time - self.last_warning_time[warning_type] >= self.warning_cooldown:
            self.warnings.append(message)
            self.last_warning_time[warning_type] = current_time
            logger.warning(message)

    def add_error(self, error_message: str) -> None:
        """Añade un error al registro."""
        self.errors.append(error_message)
        logger.error(error_message)

    def get_status_summary(self) -> Dict:
        """Retorna un resumen del estado actual."""
        return {
            'warnings': self.warnings.copy(),
            'errors': self.errors.copy(),
            'warning_count': len(self.warnings),
            'error_count': len(self.errors)
        }

    def clear_warnings(self) -> None:
        """Limpia las advertencias acumuladas."""
        self.warnings = []

    def clear_errors(self) -> None:
        """Limpia los errores acumulados."""
        self.errors = []
