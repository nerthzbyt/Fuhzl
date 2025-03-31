import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List
import logging
from collections import deque

logger = logging.getLogger("NertzMetalEngine")

class UnifiedLogger:
    def __init__(self, log_dir: str):
        self.log_dir = log_dir
        self.master_file = os.path.join(log_dir, 'master_trading_log.json')
        self.current_session: Optional[str] = None
        self.market_data_buffer: Dict[str, deque] = {}
        self.last_orderbook_update: Dict[str, datetime] = {}
        self.last_ticker_update: Dict[str, datetime] = {}
        self.update_threshold = timedelta(seconds=5)
        self.buffer_size = 1000
        self._ensure_master_file()

    def _ensure_master_file(self) -> None:
        os.makedirs(self.log_dir, exist_ok=True)
        if not os.path.exists(self.master_file):
            with open(self.master_file, 'w') as f:
                json.dump({
                    "sessions": {},
                    "global_stats": {
                        "total_sessions": 0,
                        "total_trades": 0,
                        "total_profit": 0.0,
                        "total_loss": 0.0,
                        "net_profit": 0.0,
                        "best_trade": None,
                        "worst_trade": None,
                        "win_rate": 0.0,
                        "last_update": None
                    }
                }, f, indent=4)

    def _load_master_data(self) -> Dict[str, Any]:
        try:
            with open(self.master_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error al cargar el archivo maestro: {e}")
            return {"sessions": {}}

    def _save_master_data(self, data: Dict[str, Any]) -> None:
        try:
            with open(self.master_file, 'w') as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            logger.error(f"Error al guardar el archivo maestro: {e}")

    def start_session(self, session_id: str) -> None:
        self.current_session = session_id
        master_data = self._load_master_data()
        
        # Siempre crear una nueva sesión limpia
        master_data["sessions"][session_id] = {
            "metadata": {
                "session_start": session_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "capital_inicial": 0.0,
                "capital_actual": 0.0,
                "capital_final": 0.0,
                "total_pnl": 0.0,
                "total_trades": 0,
                "iterations": 0,
                "running": True,
                "last_trade_timestamp": None
            },
            "summary": {
                "total_profit": 0.0,
                "total_loss": 0.0,
                "net_profit": 0.0,
                "win_rate": 0.0,
                "avg_profit_per_trade": 0.0
            },
            "by_symbol": {},
            "trades": {}
        }
        self._save_master_data(master_data)

    def update_session_data(self, results: Dict[str, Any]) -> None:
        if not self.current_session:
            logger.error("No hay sesión activa para actualizar")
            return

        master_data = self._load_master_data()
        session_data = master_data["sessions"].get(self.current_session, {})

        # Actualizar metadata
        session_data["metadata"].update(results["metadata"])
        
        # Actualizar resumen
        session_data["summary"] = results["summary"]
        
        # Actualizar datos por símbolo
        session_data["by_symbol"] = results["by_symbol"]
        
        # Actualizar trades
        for symbol, trades in results["trades"].items():
            if symbol not in session_data["trades"]:
                session_data["trades"][symbol] = []
            
            existing_trades = {t["trade_id"]: t for t in session_data["trades"][symbol]}
            for trade in trades:
                existing_trades[trade["trade_id"]] = trade
            
            session_data["trades"][symbol] = list(existing_trades.values())

        # Actualizar estadísticas globales
        global_stats = master_data.get("global_stats", {})
        if global_stats:
            global_stats["total_trades"] += len(trades)
            global_stats["total_profit"] += results["summary"]["total_profit"]
            global_stats["total_loss"] += results["summary"]["total_loss"]
            global_stats["net_profit"] = global_stats["total_profit"] - global_stats["total_loss"]
            
            # Actualizar mejor/peor trade
            for symbol, trades in results["trades"].items():
                for trade in trades:
                    if trade.get("profit", 0) > 0:
                        if not global_stats["best_trade"] or trade["profit"] > global_stats["best_trade"]["profit"]:
                            global_stats["best_trade"] = trade
                    elif trade.get("profit", 0) < 0:
                        if not global_stats["worst_trade"] or trade["profit"] < global_stats["worst_trade"]["profit"]:
                            global_stats["worst_trade"] = trade
            
            # Actualizar win rate global
            if global_stats["total_trades"] > 0:
                profitable_trades = sum(1 for t in trades if t.get("profit", 0) > 0)
                global_stats["win_rate"] = (profitable_trades / global_stats["total_trades"]) * 100
            
            global_stats["last_update"] = datetime.now(timezone.utc).isoformat()

        master_data["sessions"][self.current_session] = session_data
        master_data["global_stats"] = global_stats
        self._save_master_data(master_data)

    def get_session_data(self, session_id: Optional[str] = None) -> Dict[str, Any]:
        master_data = self._load_master_data()
        session_id = session_id or self.current_session
        
        if not session_id:
            logger.error("No se especificó ID de sesión")
            return {}
            
        return master_data["sessions"].get(session_id, {})

    def get_all_sessions(self) -> Dict[str, Any]:
        return self._load_master_data()["sessions"]

    def log_market_data(self, symbol: str, data_type: str, data: Dict[str, Any]) -> None:
        current_time = datetime.now(timezone.utc)

        if symbol not in self.market_data_buffer:
            self.market_data_buffer[symbol] = deque(maxlen=self.buffer_size)
            self.last_orderbook_update[symbol] = current_time - self.update_threshold
            self.last_ticker_update[symbol] = current_time - self.update_threshold

        if data_type == "orderbook":
            if current_time - self.last_orderbook_update.get(symbol, current_time) >= self.update_threshold:
                self.market_data_buffer[symbol].append({
                    "timestamp": current_time.isoformat(),
                    "type": "orderbook",
                    "data": {
                        "bids_count": data.get("bids_count", 0),
                        "asks_count": data.get("asks_count", 0)
                    }
                })
                self.last_orderbook_update[symbol] = current_time
                logger.info(f"🤘 Orderbook guardado para {symbol}: Bids={data.get('bids_count', 0)}, Asks={data.get('asks_count', 0)}")

        elif data_type == "ticker":
            if current_time - self.last_ticker_update.get(symbol, current_time) >= self.update_threshold:
                self.market_data_buffer[symbol].append({
                    "timestamp": current_time.isoformat(),
                    "type": "ticker",
                    "data": {
                        "last": data.get("last", 0.0),
                        "usd_index": data.get("usd_index", 0.0)
                    }
                })
                self.last_ticker_update[symbol] = current_time
                logger.info(f"⚡ Ticker actualizado para {symbol}: Last={data.get('last', 0.0)}, USDIndex={data.get('usd_index', 0.0)}")

    def get_market_data(self, symbol: str, data_type: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        if symbol not in self.market_data_buffer:
            return []

        data = list(self.market_data_buffer[symbol])
        if data_type:
            data = [entry for entry in data if entry["type"] == data_type]
        
        return data[-limit:] if limit > 0 else data

    def analyze_trading_patterns(self, session_id: Optional[str] = None) -> Dict[str, Any]:
        """Analiza patrones de trading y calcula métricas avanzadas."""
        session_data = self.get_session_data(session_id)
        if not session_data:
            return {}

        analysis = {
            "patterns": {},
            "performance_metrics": {},
            "risk_metrics": {},
            "market_conditions": {}
        }

        for symbol, trades in session_data.get("trades", {}).items():
            if not trades:
                continue

            # Análisis de patrones de entrada/salida
            entry_patterns = self._analyze_entry_patterns(trades)
            exit_patterns = self._analyze_exit_patterns(trades)
            analysis["patterns"][symbol] = {
                "entry": entry_patterns,
                "exit": exit_patterns
            }

            # Métricas avanzadas de rendimiento
            performance = self._calculate_advanced_metrics(trades)
            analysis["performance_metrics"][symbol] = performance

            # Métricas de riesgo
            risk_metrics = self._calculate_risk_metrics(trades)
            analysis["risk_metrics"][symbol] = risk_metrics

            # Análisis de condiciones de mercado
            market_conditions = self._analyze_market_conditions(symbol)
            analysis["market_conditions"][symbol] = market_conditions

        return analysis

    def _analyze_entry_patterns(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analiza patrones de entrada en las operaciones."""
        patterns = {
            "time_based": {},
            "price_based": {},
            "indicator_based": {}
        }

        for trade in trades:
            if trade.get("action") == "buy":
                # Análisis temporal
                timestamp = datetime.fromisoformat(trade["timestamp"])
                hour = timestamp.hour
                patterns["time_based"][hour] = patterns["time_based"].get(hour, 0) + 1

                # Análisis de precio
                entry_price = trade.get("entry_price")
                if entry_price:
                    price_level = round(entry_price / 1000) * 1000
                    patterns["price_based"][price_level] = patterns["price_based"].get(price_level, 0) + 1

                # Análisis de indicadores
                indicators = {
                    "combined": trade.get("combined"),
                    "ild": trade.get("ild"),
                    "egm": trade.get("egm"),
                    "rol": trade.get("rol"),
                    "pio": trade.get("pio"),
                    "ogm": trade.get("ogm")
                }
                patterns["indicator_based"] = self._update_indicator_patterns(patterns["indicator_based"], indicators)

        return patterns

    def _analyze_exit_patterns(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analiza patrones de salida en las operaciones."""
        patterns = {
            "profit_zones": {},
            "loss_zones": {},
            "time_held": {}
        }

        for trade in trades:
            if trade.get("exit_price") and trade.get("entry_price"):
                profit_loss = trade.get("profit_loss", 0)
                time_diff = (datetime.fromisoformat(trade.get("exit_timestamp", trade["timestamp"])) -
                           datetime.fromisoformat(trade["timestamp"])).total_seconds() / 3600

                # Análisis de zonas de beneficio/pérdida
                if profit_loss > 0:
                    zone = "profit"
                    patterns["profit_zones"][round(profit_loss, 2)] = patterns["profit_zones"].get(round(profit_loss, 2), 0) + 1
                else:
                    zone = "loss"
                    patterns["loss_zones"][round(profit_loss, 2)] = patterns["loss_zones"].get(round(profit_loss, 2), 0) + 1

                # Análisis de tiempo de retención
                time_category = f"{int(time_diff)}h"
                if time_category not in patterns["time_held"]:
                    patterns["time_held"][time_category] = {"count": 0, "profit": 0, "loss": 0}
                patterns["time_held"][time_category]["count"] += 1
                patterns["time_held"][time_category][zone] += abs(profit_loss)

        return patterns

    def _calculate_advanced_metrics(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula métricas avanzadas de rendimiento."""
        metrics = {
            "profit_factor": 0,
            "sharpe_ratio": 0,
            "max_drawdown": 0,
            "win_streak": 0,
            "loss_streak": 0,
            "avg_win_size": 0,
            "avg_loss_size": 0,
            "risk_reward_ratio": 0
        }

        if not trades:
            return metrics

        total_profit = sum(t.get("profit_loss", 0) for t in trades if t.get("profit_loss", 0) > 0)
        total_loss = abs(sum(t.get("profit_loss", 0) for t in trades if t.get("profit_loss", 0) < 0))

        metrics["profit_factor"] = total_profit / total_loss if total_loss > 0 else float("inf")
        
        # Cálculo de ratio de Sharpe simplificado
        returns = [t.get("profit_loss", 0) for t in trades]
        if returns:
            avg_return = sum(returns) / len(returns)
            std_dev = (sum((r - avg_return) ** 2 for r in returns) / len(returns)) ** 0.5
            metrics["sharpe_ratio"] = avg_return / std_dev if std_dev > 0 else 0

        # Cálculo de drawdown máximo
        cumulative = 0
        peak = 0
        drawdown = 0
        for trade in trades:
            cumulative += trade.get("profit_loss", 0)
            peak = max(peak, cumulative)
            drawdown = min(drawdown, cumulative - peak)
        metrics["max_drawdown"] = abs(drawdown)

        return metrics

    def _calculate_risk_metrics(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calcula métricas de riesgo."""
        return {
            "value_at_risk": self._calculate_var(trades),
            "risk_per_trade": self._calculate_risk_per_trade(trades),
            "exposure_time": self._calculate_exposure_time(trades)
        }

    def _analyze_market_conditions(self, symbol: str) -> Dict[str, Any]:
        """Analiza las condiciones del mercado durante las operaciones."""
        market_data = self.get_market_data(symbol, limit=1000)
        conditions = {
            "volatility": self._calculate_volatility(market_data),
            "trend": self._analyze_trend(market_data),
            "volume_profile": self._analyze_volume(market_data)
        }
        return conditions

    def _update_indicator_patterns(self, current_patterns: Dict[str, Any], indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Actualiza los patrones basados en indicadores."""
        for indicator, value in indicators.items():
            if value is not None:
                if indicator not in current_patterns:
                    current_patterns[indicator] = {"sum": 0, "count": 0, "min": float("inf"), "max": float("-inf")}
                current_patterns[indicator]["sum"] += value
                current_patterns[indicator]["count"] += 1
                current_patterns[indicator]["min"] = min(current_patterns[indicator]["min"], value)
                current_patterns[indicator]["max"] = max(current_patterns[indicator]["max"], value)
        return current_patterns

    def _calculate_var(self, trades: List[Dict[str, Any]], confidence: float = 0.95) -> float:
        """Calcula el Valor en Riesgo (VaR) para un nivel de confianza dado."""
        if not trades:
            return 0.0
        returns = sorted([t.get("profit_loss", 0) for t in trades])
        index = int((1 - confidence) * len(returns))
        return abs(returns[index]) if index < len(returns) else 0.0

    def _calculate_risk_per_trade(self, trades: List[Dict[str, Any]]) -> float:
        """Calcula el riesgo promedio por operación."""
        if not trades:
            return 0.0
        risks = [t.get("risk_reward_ratio", 0) * abs(t.get("profit_loss", 0)) for t in trades]
        return sum(risks) / len(risks) if risks else 0.0

    def _calculate_exposure_time(self, trades: List[Dict[str, Any]]) -> Dict[str, float]:
        """Calcula el tiempo de exposición al mercado."""
        total_time = 0
        max_time = 0
        for trade in trades:
            entry_time = datetime.fromisoformat(trade["timestamp"])
            exit_time = datetime.fromisoformat(trade.get("exit_timestamp", trade["timestamp"]))
            duration = (exit_time - entry_time).total_seconds() / 3600
            total_time += duration
            max_time = max(max_time, duration)

        return {
            "average_exposure": total_time / len(trades) if trades else 0,
            "max_exposure": max_time
        }

    def _calculate_volatility(self, market_data: List[Dict[str, Any]]) -> float:
        """Calcula la volatilidad del mercado."""
        if not market_data:
            return 0.0
        prices = [entry["data"]["last"] for entry in market_data if entry["type"] == "ticker" and "last" in entry["data"]]
        if len(prices) < 2:
            return 0.0
        returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
        return (sum(r * r for r in returns) / (len(returns) - 1)) ** 0.5 if len(returns) > 1 else 0.0

    def _analyze_trend(self, market_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analiza la tendencia del mercado."""
        if not market_data:
            return {"direction": "neutral", "strength": 0}

        prices = [entry["data"]["last"] for entry in market_data if entry["type"] == "ticker" and "last" in entry["data"]]
        if len(prices) < 2:
            return {"direction": "neutral", "strength": 0}

        # Cálculo simple de tendencia
        price_change = prices[-1] - prices[0]
        strength = abs(price_change) / prices[0]
        direction = "upward" if price_change > 0 else "downward" if price_change < 0 else "neutral"

        return {
            "direction": direction,
            "strength": strength
        }

    def _analyze_volume(self, market_data: List[Dict[str, Any]]) -> Dict[str, float]:
        """Analiza el perfil de volumen."""
        if not market_data:
            return {"average_volume": 0, "volume_trend": 0}

        volumes = []
        for entry in market_data:
            if entry["type"] == "ticker" and "last" in entry["data"]:
                # Aquí podrías agregar lógica para calcular el volumen si está disponible
                volumes.append(1)  # Placeholder para el volumen

        if not volumes:
            return {"average_volume": 0, "volume_trend": 0}

        avg_volume = sum(volumes) / len(volumes)
        volume_trend = (sum(volumes[-10:]) / 10) / avg_volume if len(volumes) >= 10 else 1

        return {
            "average_volume": avg_volume,
            "volume_trend": volume_trend
        }