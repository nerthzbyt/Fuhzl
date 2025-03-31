import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger("NertzMetalEngine")


class RateLimitManager:
    def __init__(self):
        self.ip_limits: Dict[str, Dict] = {
            "order": {"count": 0, "reset_time": 0, "limit": 100},  # 100 requests per minute
            "trade": {"count": 0, "reset_time": 0, "limit": 600},  # 600 requests per minute
            "position": {"count": 0, "reset_time": 0, "limit": 120}  # 120 requests per minute
        }
        self.uid_limits: Dict[str, Dict] = {
            "order": {"count": 0, "reset_time": 0, "limit": 100},
            "trade": {"count": 0, "reset_time": 0, "limit": 600},
            "position": {"count": 0, "reset_time": 0, "limit": 120}
        }
        self.backoff_time = 1  # Tiempo inicial de espera en segundos
        self.max_backoff_time = 32  # Tiempo máximo de espera en segundos

    def _should_reset(self, reset_time: float) -> bool:
        """Verifica si se debe reiniciar el contador basado en el tiempo."""
        current_time = datetime.now(timezone.utc).timestamp()
        return current_time >= reset_time

    def _reset_if_needed(self, limits: Dict[str, Dict], category: str) -> None:
        """Reinicia los contadores si es necesario."""
        if self._should_reset(limits[category]["reset_time"]):
            limits[category]["count"] = 0
            limits[category]["reset_time"] = datetime.now(timezone.utc).timestamp() + 60

    def check_rate_limit(self, category: str, is_ip_limit: bool = True) -> bool:
        """Verifica si se ha alcanzado el límite de tasa."""
        limits = self.ip_limits if is_ip_limit else self.uid_limits
        self._reset_if_needed(limits, category)

        if limits[category]["count"] >= limits[category]["limit"]:
            return False

        limits[category]["count"] += 1
        return True

    async def handle_rate_limit(self, category: str) -> None:
        """Maneja los límites de tasa con backoff exponencial."""
        while not (self.check_rate_limit(category, True) and
                   self.check_rate_limit(category, False)):
            logger.warning(f"Rate limit alcanzado para {category}. "
                           f"Esperando {self.backoff_time} segundos.")
            time.sleep(self.backoff_time)
            self.backoff_time = min(self.backoff_time * 2, self.max_backoff_time)

        # Resetear el tiempo de backoff después de una solicitud exitosa
        self.backoff_time = 1

    def handle_429_response(self) -> None:
        """Maneja la respuesta 429 (Too Many Requests)."""
        logger.warning(f"Recibido error 429. Esperando {self.backoff_time} segundos.")
        time.sleep(self.backoff_time)
        self.backoff_time = min(self.backoff_time * 2, self.max_backoff_time)

    def update_limits(self, headers: Optional[Dict] = None) -> None:
        """Actualiza los límites basados en las cabeceras de respuesta."""
        if not headers:
            return

        # Actualizar límites basados en las cabeceras de la respuesta
        if 'x-ratelimit-remaining-order-ip' in headers:
            remaining = int(headers['x-ratelimit-remaining-order-ip'])
            self.ip_limits['order']['limit'] = remaining

        if 'x-ratelimit-remaining-trade-ip' in headers:
            remaining = int(headers['x-ratelimit-remaining-trade-ip'])
            self.ip_limits['trade']['limit'] = remaining

        if 'x-ratelimit-remaining-position-ip' in headers:
            remaining = int(headers['x-ratelimit-remaining-position-ip'])
            self.ip_limits['position']['limit'] = remaining
