from datetime import datetime
from typing import Optional

from sqlalchemy import Integer, String, Float, DateTime, JSON
from sqlalchemy.orm import declarative_base, Mapped
from sqlalchemy.orm import MappedColumn as mapped_column

Base = declarative_base()


class MarketData(Base):
    __tablename__ = "market_data"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True, nullable=False)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False)
    open: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    high: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    low: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    close: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    volume: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class Orderbook(Base):
    __tablename__ = "orderbook"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    bids: Mapped[dict] = mapped_column(JSON, nullable=False)
    asks: Mapped[dict] = mapped_column(JSON, nullable=False)


class MarketTicker(Base):
    __tablename__ = "market_ticker"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    last_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    volume_24h: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    high_24h: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    low_24h: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)


class Trade(Base):
    __tablename__ = "trades"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    trade_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String, nullable=False)
    entry_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    exit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    quantity: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    profit_loss: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    decision: Mapped[str] = mapped_column(String, nullable=False)
    combined: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ild: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    egm: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    rol: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    pio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ogm: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    risk_reward_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True, default=1.67)
    order_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)


class Position(Base):
    __tablename__ = "positions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    order_id: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String, nullable=False)
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    tp: Mapped[float] = mapped_column(Float, nullable=False)
    sl: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False, default="open")
