import logging
from typing import Dict, List, Tuple

from scripts.settings import ConfigSettings
from scripts.utils import calculate_metrics, calculate_tp_sl

logger = logging.getLogger("NertzMetalEngine")
config = ConfigSettings()


class BaseStrategy:
    def __init__(self):
        self.name = "base_strategy"
        self.description = "Estrategia base para el trading"

    def analyze_market(self, symbol: str, candle_data: List[Dict], orderbook: Dict, ticker: Dict) -> Dict:
        """Analiza las condiciones del mercado y retorna métricas con análisis de balance."""
        metrics = calculate_metrics(candle_data, orderbook, ticker)

        # Análisis de balance y rendimiento
        last_price = float(ticker.get('last_price', 0))
        volume_24h = float(ticker.get('volume_24h', 0))

        # Calcular métricas de balance
        metrics['market_value'] = last_price * volume_24h
        metrics['market_strength'] = metrics['combined'] * (1 + abs(metrics['ild']))
        metrics['risk_adjusted_return'] = metrics['market_strength'] / (1 + metrics['volatility'])

        return metrics

    def should_trade(self, metrics: Dict, available_btc: float) -> Tuple[str, str]:
        """Determina si se debe realizar una operación basada en las métricas y el estado del mercado."""
        # Extracción de métricas con valores por defecto seguros
        combined = metrics.get('combined', 0)
        ild = metrics.get('ild', 0)
        egm = metrics.get('egm', 0)
        rol = metrics.get('rol', 0)
        pio = metrics.get('pio', 0)
        ogm = metrics.get('ogm', 0)
        volatility = metrics.get('volatility', 0.01)
        open_positions = metrics.get('open_positions', 0)
        last_trade_time = metrics.get('last_trade_time', 0)
        current_time = metrics.get('current_time', 0)
        failed_trades_count = metrics.get('failed_trades_count', 0)
        spread = metrics.get('spread', 0)
        market_depth = metrics.get('market_depth', 0)
        volume_24h = metrics.get('volume_24h', 0)
        sync_status = metrics.get('sync_status', '')
        market_state = metrics.get('market_state', 'NEUTRAL')
        trend_strength = metrics.get('trend_strength', 0)

        # Validación del estado de sincronización
        if sync_status == 'SYNCHRONIZING':
            return "hold", "Sistema en sincronización"

        # Validación avanzada de condiciones de mercado
        if volatility > config.MAX_VOLATILITY:
            if market_state != 'TENDENCIA_ALCISTA' and trend_strength < 0.5:
                return "hold", "Volatilidad demasiado alta sin tendencia clara"

        # Validación de spread y profundidad de mercado con ajustes dinámicos
        spread_limit = config.MAX_SPREAD
        if market_state == 'ALTA_VOLATILIDAD':
            spread_limit *= 0.8  # Más restrictivo en mercados volátiles

        if spread > spread_limit:
            return "hold", f"Spread superior al límite ajustado: {spread_limit:.4f}"

        depth_requirement = config.MIN_MARKET_DEPTH
        if market_state in ['TENDENCIA_ALCISTA', 'TENDENCIA_BAJISTA']:
            depth_requirement *= 1.2  # Mayor profundidad requerida en tendencias fuertes

        if market_depth < depth_requirement:
            return "hold", f"Profundidad de mercado insuficiente: {market_depth:.2f} < {depth_requirement:.2f}"

        if volume_24h < config.MIN_24H_VOLUME:
            return "hold", "Volumen insuficiente"

        # Validación de posiciones abiertas
        if open_positions >= config.MAX_OPEN_ORDERS:
            return "hold", "Máximo de posiciones abiertas alcanzado"

        # Control de errores consecutivos
        if failed_trades_count >= 3:
            return "hold", "Demasiados errores consecutivos"

        # Período de enfriamiento dinámico basado en volatilidad
        cooldown_time = config.TRADE_COOLDOWN * (1 + volatility)
        if last_trade_time + cooldown_time > current_time:
            return "hold", "En período de espera adaptativo"

        # Validación más estricta para señales de trading con múltiples confirmaciones
        if (combined > 2.0 and
                ild > 0.2 and
                egm > 0.15 and
                rol > 0 and
                pio > config.PIO_THRESHOLD * 1.2 and
                ogm > 0.5 and
                spread <= config.TARGET_SPREAD and
                market_depth >= config.TARGET_MARKET_DEPTH):
            return "buy", "Señales alcistas fuertes confirmadas"
        elif (combined < -2.0 and
              ild < -0.2 and
              egm < -0.15 and
              rol < 0 and
              pio < -config.PIO_THRESHOLD * 1.2 and
              ogm > 0.5):
            return "sell", "Señales bajistas fuertes confirmadas"
        return "hold", "Sin señales claras o insuficientes confirmaciones"

    def calculate_position_size(self, capital: float, price: float, volatility: float) -> float:
        """Calcula el tamaño de la posición optimizado para balance y riesgo."""
        # Factor de riesgo dinámico basado en volatilidad
        dynamic_risk = config.RISK_FACTOR * (1 / (1 + volatility))

        # Ajuste por capital disponible
        risk_amount = capital * dynamic_risk

        # Cálculo de posición con protección adicional
        position_size = risk_amount / (price * max(volatility, 0.01))

        # Límites de tamaño con margen de seguridad
        max_position = min(config.MAX_TRADE_SIZE, capital / (price * 2))
        return min(max(position_size, config.MIN_TRADE_SIZE), max_position)

    def get_entry_points(self, price: float, volatility: float, action: str) -> Tuple[float, float, float]:
        """Calcula los puntos de entrada, take profit y stop loss."""
        tp, sl = calculate_tp_sl(price, volatility, action)
        return price, tp, sl


class ConservativeStrategy(BaseStrategy):
    def __init__(self):
        super().__init__()
        self.name = "conservative_strategy"
        self.description = "Estrategia conservadora con menor riesgo"

    def should_trade(self, metrics: Dict, available_btc: float) -> Tuple[str, str]:
        combined = metrics.get('combined', 0)
        ild = metrics.get('ild', 0)
        egm = metrics.get('egm', 0)
        rol = metrics.get('rol', 0)

        if combined > 2.0 and ild > 0.2 and egm > 0.1 and rol > 0:
            return "buy", "Múltiples confirmaciones alcistas"
        elif combined < -2.0 and ild < -0.2 and egm < -0.1 and rol < 0:
            return "sell", "Múltiples confirmaciones bajistas"
        return "hold", "Esperando mejores condiciones"


class AdaptiveStrategy(BaseStrategy):
    def __init__(self):
        super().__init__()
        self.name = "adaptive_strategy"
        self.description = "Estrategia adaptativa que se ajusta a las condiciones del mercado"
        self.volatility_threshold = 0.15
        self.trend_strength_threshold = 0.1

    def should_trade(self, metrics: Dict, available_btc: float) -> Tuple[str, str]:
        combined = metrics.get('combined', 0)
        pio = metrics.get('pio', 0)
        volatility = metrics.get('volatility', 0.01)
        trend_strength = metrics.get('trend_strength', 0)
        market_depth = metrics.get('market_depth', 0)
        volume_24h = metrics.get('volume_24h', 0)

        # Validación de condiciones de mercado
        if volatility > self.volatility_threshold:
            if market_depth >= config.TARGET_MARKET_DEPTH * 1.5 and volume_24h >= config.MIN_24H_VOLUME * 1.5:
                # Mercado volátil pero con buena liquidez - modo agresivo
                if combined > 2.0 and pio > 0.15 and trend_strength > self.trend_strength_threshold:
                    return "buy", "Oportunidad agresiva en mercado volátil"
                elif combined < -2.0 and pio < -0.15 and trend_strength < -self.trend_strength_threshold:
                    return "sell", "Venta agresiva en mercado volátil"
            else:
                # Mercado volátil con baja liquidez - modo conservador
                return "hold", "Mercado volátil con baja liquidez"
        else:
            # Mercado estable - modo normal
            if combined > 1.8 and pio > 0.12 and trend_strength > 0:
                return "buy", "Oportunidad de compra en mercado estable"
            elif combined < -1.8 and pio < -0.12 and trend_strength < 0:
                return "sell", "Oportunidad de venta en mercado estable"

        return "hold", "Esperando mejores condiciones"

    def calculate_position_size(self, capital: float, price: float, volatility: float) -> float:
        """Calcula un tamaño de posición adaptativo basado en las condiciones del mercado."""
        # Factor de riesgo dinámico
        dynamic_risk = config.RISK_FACTOR
        if volatility <= self.volatility_threshold / 2:
            dynamic_risk *= 1.2  # Aumentar riesgo en mercados estables
        elif volatility > self.volatility_threshold:
            dynamic_risk *= 0.8  # Reducir riesgo en mercados volátiles

        risk_amount = capital * dynamic_risk
        position_size = risk_amount / (price * max(volatility, 0.01))

        # Límites adaptativos
        max_size = config.MAX_TRADE_SIZE
        if volatility > self.volatility_threshold:
            max_size *= 0.8  # Reducir tamaño máximo en mercados volátiles

        return min(max(position_size, config.MIN_TRADE_SIZE), max_size)
