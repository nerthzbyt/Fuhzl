import logging
from typing import Dict, List
from datetime import datetime

import numpy as np

from scripts.settings import ConfigSettings
from scripts.utils import calculate_metrics

logger = logging.getLogger("NertzMetalEngine")
config = ConfigSettings()


class MarketAnalyzer:
    def __init__(self):
        self.metrics_history = {}
        self.volatility_window = 20
        self.trend_window = 10
        self.metrics_cache = {}
        self.cache_duration = 5  # segundos
        self.last_update = {}

    def analyze_market_conditions(self, symbol: str, candle_data: List[Dict],
                                  orderbook: Dict, ticker: Dict) -> Dict[str, float]:
        """Analiza las condiciones del mercado y calcula métricas avanzadas."""
        current_time = datetime.now().timestamp()
        
        # Verificar caché
        if (symbol in self.metrics_cache and
            current_time - self.last_update.get(symbol, 0) < self.cache_duration):
            return self.metrics_cache[symbol]
            
        metrics = calculate_metrics(candle_data, orderbook, ticker)

        # Añadir métricas al historial
        if symbol not in self.metrics_history:
            self.metrics_history[symbol] = []
        self.metrics_history[symbol].append(metrics)

        # Mantener solo las últimas N métricas
        if len(self.metrics_history[symbol]) > self.volatility_window:
            self.metrics_history[symbol].pop(0)

        # Calcular métricas adicionales
        metrics.update(self._calculate_advanced_metrics(symbol))
        
        # Actualizar caché
        self.metrics_cache[symbol] = metrics
        self.last_update[symbol] = current_time
        
        return metrics

    def _calculate_advanced_metrics(self, symbol: str) -> Dict[str, float]:
        """Calcula métricas avanzadas basadas en el historial."""
        if not self.metrics_history.get(symbol):
            return {}

        history = self.metrics_history[symbol]
        combined_values = [m['combined'] for m in history]
        volume_values = [m.get('volume', 0) for m in history]
        price_values = [m.get('price', 0) for m in history]

        metrics = {
            'trend_strength': self._calculate_trend_strength(combined_values),
            'momentum': self._calculate_momentum(combined_values),
            'volatility_index': self._calculate_volatility(combined_values),
            'volume_trend': self._calculate_volume_trend(volume_values),
            'price_momentum': self._calculate_price_momentum(price_values),
            'market_efficiency': self._calculate_market_efficiency(price_values, volume_values)
        }

        return metrics

    def _calculate_trend_strength(self, values: List[float]) -> float:
        """Calcula la fuerza de la tendencia."""
        if len(values) < self.trend_window:
            return 0.0

        recent_values = values[-self.trend_window:]
        slope = np.polyfit(range(len(recent_values)), recent_values, 1)[0]
        return float(slope)

    def _calculate_momentum(self, values: List[float]) -> float:
        """Calcula el momentum del mercado."""
        if len(values) < 2:
            return 0.0

        return values[-1] - values[0]

    def _calculate_volatility(self, values: List[float]) -> float:
        """Calcula el índice de volatilidad."""
        if len(values) < 2:
            return 0.01

        returns = np.diff(values) / values[:-1]
        return float(np.std(returns) * np.sqrt(len(values)))

    def _calculate_volume_trend(self, volumes: List[float]) -> float:
        """Analiza la tendencia del volumen."""
        if len(volumes) < self.trend_window:
            return 0.0
        recent_volumes = volumes[-self.trend_window:]
        return float(np.mean(recent_volumes) / np.mean(volumes))

    def _calculate_price_momentum(self, prices: List[float]) -> float:
        """Calcula el momentum del precio con ponderación exponencial."""
        if len(prices) < 2:
            return 0.0
        weights = np.exp(np.linspace(-1., 0., len(prices)))
        weighted_returns = np.diff(prices) * weights[1:]
        return float(np.sum(weighted_returns))

    def _calculate_market_efficiency(self, prices: List[float], volumes: List[float]) -> float:
        """Calcula la eficiencia del mercado basada en precio y volumen."""
        if len(prices) < 2 or len(volumes) < 2:
            return 0.5
        price_efficiency = abs(prices[-1] - prices[0]) / sum(abs(p2-p1) for p1, p2 in zip(prices[:-1], prices[1:]))
        volume_impact = np.corrcoef(np.diff(prices), volumes[1:])[0,1] if len(prices) > 2 else 0
        return float(0.5 * (price_efficiency + abs(volume_impact)))

    def get_market_state(self, metrics: Dict[str, float]) -> str:
        """Determina el estado actual del mercado usando métricas avanzadas."""
        combined = metrics.get('combined', 0)
        trend_strength = metrics.get('trend_strength', 0)
        momentum = metrics.get('momentum', 0)
        volatility = metrics.get('volatility_index', 0.01)
        volume_trend = metrics.get('volume_trend', 1.0)
        price_momentum = metrics.get('price_momentum', 0)
        market_efficiency = metrics.get('market_efficiency', 0.5)

        # Evaluación ponderada de condiciones
        trend_score = trend_strength * volume_trend
        momentum_score = momentum * market_efficiency
        risk_score = volatility * (1 - market_efficiency)

        if risk_score > 2.0:
            return "ALTA_VOLATILIDAD"
        elif trend_score > 1.5 and momentum_score > 0:
            return "TENDENCIA_ALCISTA_FUERTE" if volume_trend > 1.2 else "TENDENCIA_ALCISTA"
        elif trend_score < -1.5 and momentum_score < 0:
            return "TENDENCIA_BAJISTA_FUERTE" if volume_trend > 1.2 else "TENDENCIA_BAJISTA"
        elif abs(trend_score) < 0.5 and risk_score < 0.8:
            return "CONSOLIDACIÓN"
        elif market_efficiency > 0.7:
            return "MERCADO_EFICIENTE"
        return "NEUTRAL"

    def should_adjust_parameters(self, market_state: str) -> Dict[str, float]:
        """Ajusta los parámetros de trading según el estado del mercado."""
        adjustments = {
            'risk_factor': 1.0,
            'tp_factor': 1.0,
            'sl_factor': 1.0
        }

        if market_state == "ALTA_VOLATILIDAD":
            adjustments['risk_factor'] = 0.7  # Reducir riesgo
            adjustments['tp_factor'] = 1.2  # Aumentar take profit
            adjustments['sl_factor'] = 0.8  # Reducir stop loss
        elif market_state == "TENDENCIA_ALCISTA":
            adjustments['tp_factor'] = 1.3  # Mayor take profit
            adjustments['risk_factor'] = 1.1  # Ligero aumento de riesgo
        elif market_state == "TENDENCIA_BAJISTA":
            adjustments['sl_factor'] = 1.2  # Mayor protección
            adjustments['risk_factor'] = 0.9  # Reducir riesgo

        return adjustments
