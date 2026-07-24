"""
WEEX 山寨幣拉盤監控系統 v2.0
核心引擎 - 穩定運行、自動重連、莊家拉盤偵測
"""

import asyncio
import json
import time
import logging
import traceback
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable
from collections import deque
import numpy as np

import websockets
import aiohttp

# 配置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('tracker.log')
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class PumpSignal:
    """拉盤信號數據結構"""
    symbol: str
    timestamp: float
    price: float
    volume_24h: float
    pump_strength: float  # 0-100
    volume_surge_ratio: float  # 量比
    price_change_1m: float
    price_change_5m: float
    price_change_15m: float
    order_book_imbalance: float  # 買賣盤失衡度
    whale_buy_pressure: float  # 大單買壓
    momentum_score: float  # 動能分數
    recommendation: str  # BUY / HOLD / SELL / WAIT
    entry_price: Optional[float] = None
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None
    confidence: float = 0.0  # 信心度

    def to_dict(self):
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'price': self.price,
            'volume_24h': self.volume_24h,
            'pump_strength': round(self.pump_strength, 2),
            'volume_surge_ratio': round(self.volume_surge_ratio, 2),
            'price_change_1m': round(self.price_change_1m, 4),
            'price_change_5m': round(self.price_change_5m, 4),
            'price_change_15m': round(self.price_change_15m, 4),
            'order_book_imbalance': round(self.order_book_imbalance, 4),
            'whale_buy_pressure': round(self.whale_buy_pressure, 4),
            'momentum_score': round(self.momentum_score, 2),
            'recommendation': self.recommendation,
            'entry_price': self.entry_price,
            'take_profit': self.take_profit,
            'stop_loss': self.stop_loss,
            'confidence': round(self.confidence, 2),
            'time_str': datetime.fromtimestamp(self.timestamp).strftime('%Y-%m-%d %H:%M:%S')
        }


class MarketDataBuffer:
    """市場數據緩衝區 - 儲存歷史K線用於計算指標"""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.buffers: Dict[str, deque] = {}
        self.last_update: Dict[str, float] = {}

    def add(self, symbol: str, data: dict):
        if symbol not in self.buffers:
            self.buffers[symbol] = deque(maxlen=self.max_size)
        self.buffers[symbol].append({
            'timestamp': data.get('timestamp', time.time()),
            'price': float(data.get('last', 0)),
            'volume': float(data.get('volume', 0)),
            'high': float(data.get('high', data.get('last', 0))),
            'low': float(data.get('low', data.get('last', 0))),
            'open': float(data.get('open', data.get('last', 0))),
        })
        self.last_update[symbol] = time.time()

    def get_history(self, symbol: str, minutes: int) -> List[dict]:
        """獲取最近 N 分鐘的數據"""
        if symbol not in self.buffers:
            return []
        cutoff = time.time() - minutes * 60
        return [d for d in self.buffers[symbol] if d['timestamp'] >= cutoff]

    def get_price_change(self, symbol: str, minutes: int) -> float:
        """計算 N 分鐘價格變化率"""
        history = self.get_history(symbol, minutes)
        if len(history) < 2:
            return 0.0
        old_price = history[0]['price']
        new_price = history[-1]['price']
        if old_price == 0:
            return 0.0
        return (new_price - old_price) / old_price

    def get_volume_stats(self, symbol: str, minutes: int) -> dict:
        """獲取成交量統計"""
        history = self.get_history(symbol, minutes)
        if not history:
            return {'total': 0, 'avg': 0}
        volumes = [d['volume'] for d in history]
        return {
            'total': sum(volumes),
            'avg': np.mean(volumes) if volumes else 0,
            'max': max(volumes) if volumes else 0
        }


class PumpDetector:
    """
    莊家拉盤偵測引擎
    核心邏輯：
    1. 量比突增（成交量相對平均值暴增）
    2. 價格快速上漲（1分鐘、5分鐘、15分鐘變化率）
    3. 買賣盤失衡（買盤遠大於賣盤）
    4. 大單買壓（大額買單湧入）
    5. 動能分數（綜合評估持續性）
    """

    def __init__(self):
        self.buffer = MarketDataBuffer(max_size=2000)
        self.order_books: Dict[str, dict] = {}
        self.recent_trades: Dict[str, deque] = {}
        self.active_positions: Dict[str, dict] = {}  # 已進場的幣種

        # 閾值配置（可調整）
        self.thresholds = {
            'volume_surge_min': 3.0,      # 量比至少 3 倍
            'price_change_1m_min': 0.005,  # 1分鐘漲幅 0.5%
            'price_change_5m_min': 0.015,  # 5分鐘漲幅 1.5%
            'ob_imbalance_min': 0.3,       # 買賣盤失衡 30%
            'whale_pressure_min': 0.4,     # 大單買壓 40%
            'momentum_min': 60,            # 動能分數 60
        }

    def update_ticker(self, symbol: str, data: dict):
        """更新 ticker 數據"""
        self.buffer.add(symbol, data)

    def update_order_book(self, symbol: str, bids: List, asks: List):
        """更新訂單簿"""
        self.order_books[symbol] = {
            'bids': bids,
            'asks': asks,
            'timestamp': time.time()
        }

    def update_trades(self, symbol: str, trades: List[dict]):
        """更新成交記錄"""
        if symbol not in self.recent_trades:
            self.recent_trades[symbol] = deque(maxlen=500)
        for trade in trades:
            self.recent_trades[symbol].append({
                'price': float(trade.get('price', 0)),
                'size': float(trade.get('size', 0)),
                'side': trade.get('side', 'buy'),
                'timestamp': time.time()
            })

    def calculate_order_book_imbalance(self, symbol: str) -> float:
        """計算買賣盤失衡度 (-1 到 1, 正值表示買盤強)"""
        ob = self.order_books.get(symbol)
        if not ob or not ob.get('bids') or not ob.get('asks'):
            return 0.0

        bids = ob['bids']
        asks = ob['asks']

        # 計算前5檔的總量
        bid_volume = sum(float(b[1]) for b in bids[:5] if len(b) >= 2)
        ask_volume = sum(float(a[1]) for a in asks[:5] if len(a) >= 2)

        total = bid_volume + ask_volume
        if total == 0:
            return 0.0

        return (bid_volume - ask_volume) / total

    def calculate_whale_pressure(self, symbol: str) -> float:
        """計算大單買壓 (0-1)"""
        trades = self.recent_trades.get(symbol, deque())
        if not trades:
            return 0.0

        # 只取最近 2 分鐘的成交
        cutoff = time.time() - 120
        recent = [t for t in trades if t['timestamp'] >= cutoff]

        if not recent:
            return 0.0

        # 計算大單（超過平均 3 倍）
        sizes = [t['size'] for t in recent]
        avg_size = np.mean(sizes) if sizes else 0

        whale_buys = sum(t['size'] for t in recent 
                        if t['side'] == 'buy' and t['size'] > avg_size * 3)
        whale_sells = sum(t['size'] for t in recent 
                         if t['side'] == 'sell' and t['size'] > avg_size * 3)

        total_whale = whale_buys + whale_sells
        if total_whale == 0:
            return 0.0

        return whale_buys / total_whale

    def calculate_momentum_score(self, symbol: str) -> float:
        """計算動能分數 (0-100)"""
        history = self.buffer.get_history(symbol, 15)
        if len(history) < 5:
            return 0.0

        prices = [h['price'] for h in history]

        # 1. 價格趨勢強度 (RSI-like)
        gains = []
        losses = []
        for i in range(1, len(prices)):
            diff = prices[i] - prices[i-1]
            if diff > 0:
                gains.append(diff)
            else:
                losses.append(abs(diff))

        avg_gain = np.mean(gains) if gains else 0
        avg_loss = np.mean(losses) if losses else 0.0001
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        # 2. 成交量趨勢
        volumes = [h['volume'] for h in history]
        vol_trend = 0
        if len(volumes) >= 10:
            recent_vol = np.mean(volumes[-5:])
            old_vol = np.mean(volumes[:5])
            if old_vol > 0:
                vol_trend = min((recent_vol / old_vol) * 25, 50)

        # 3. 價格連續性（是否持續上漲）
        consecutive_up = 0
        for i in range(len(prices)-1, 0, -1):
            if prices[i] > prices[i-1]:
                consecutive_up += 1
            else:
                break
        continuity_score = min(consecutive_up * 5, 25)

        # 綜合分數
        momentum = (rsi * 0.4) + vol_trend + continuity_score
        return min(momentum, 100)

    def detect_pump(self, symbol: str) -> Optional[PumpSignal]:
        """偵測拉盤信號"""
        history = self.buffer.get_history(symbol, 30)
        if len(history) < 10:
            return None

        current = history[-1]
        price = current['price']

        # 計算各項指標
        vol_stats = self.buffer.get_volume_stats(symbol, 30)
        vol_stats_recent = self.buffer.get_volume_stats(symbol, 5)

        volume_surge = 0
        if vol_stats['avg'] > 0:
            volume_surge = vol_stats_recent['avg'] / vol_stats['avg']

        price_change_1m = self.buffer.get_price_change(symbol, 1)
        price_change_5m = self.buffer.get_price_change(symbol, 5)
        price_change_15m = self.buffer.get_price_change(symbol, 15)

        ob_imbalance = self.calculate_order_book_imbalance(symbol)
        whale_pressure = self.calculate_whale_pressure(symbol)
        momentum = self.calculate_momentum_score(symbol)

        # 計算拉盤力度 (0-100)
        pump_strength = 0

        # 量比權重 25%
        if volume_surge >= self.thresholds['volume_surge_min']:
            pump_strength += min(volume_surge * 8, 25)

        # 價格漲幅權重 30%
        price_score = 0
        if price_change_1m >= self.thresholds['price_change_1m_min']:
            price_score += price_change_1m * 100 * 10
        if price_change_5m >= self.thresholds['price_change_5m_min']:
            price_score += price_change_5m * 100 * 15
        if price_change_15m > 0:
            price_score += price_change_15m * 100 * 5
        pump_strength += min(price_score, 30)

        # 買賣盤權重 20%
        if ob_imbalance >= self.thresholds['ob_imbalance_min']:
            pump_strength += ob_imbalance * 20

        # 大單權重 15%
        if whale_pressure >= self.thresholds['whale_pressure_min']:
            pump_strength += whale_pressure * 15

        # 動能權重 10%
        if momentum >= self.thresholds['momentum_min']:
            pump_strength += momentum * 0.1

        pump_strength = min(pump_strength, 100)

        # 判斷建議
        recommendation = 'WAIT'
        entry_price = None
        take_profit = None
        stop_loss = None
        confidence = 0.0

        # 進場條件
        if (volume_surge >= self.thresholds['volume_surge_min'] and
            price_change_1m >= self.thresholds['price_change_1m_min'] and
            price_change_5m >= self.thresholds['price_change_5m_min'] and
            ob_imbalance >= self.thresholds['ob_imbalance_min'] and
            momentum >= self.thresholds['momentum_min'] and
            pump_strength >= 65):

            recommendation = 'BUY'
            entry_price = price
            # 止盈：拉盤力度的 1.5-3 倍
            tp_multiplier = 1.5 + (pump_strength / 100) * 1.5
            take_profit = price * (1 + tp_multiplier / 100)
            # 止損：進場價的 -2% 到 -5%
            sl_pct = 2 + (1 - pump_strength / 100) * 3
            stop_loss = price * (1 - sl_pct / 100)
            confidence = pump_strength / 100

            # 記錄進場
            self.active_positions[symbol] = {
                'entry_price': entry_price,
                'take_profit': take_profit,
                'stop_loss': stop_loss,
                'entry_time': time.time(),
                'pump_strength_at_entry': pump_strength,
                'highest_price': price
            }

        # 已進場的監控
        elif symbol in self.active_positions:
            pos = self.active_positions[symbol]
            pos['highest_price'] = max(pos['highest_price'], price)

            # 止盈
            if price >= pos['take_profit']:
                recommendation = 'TAKE_PROFIT'
                confidence = 0.9
                del self.active_positions[symbol]
            # 止損
            elif price <= pos['stop_loss']:
                recommendation = 'STOP_LOSS'
                confidence = 0.9
                del self.active_positions[symbol]
            # 拉盤力度衰退（移動止損）
            elif pump_strength < 30 and price < pos['highest_price'] * 0.97:
                recommendation = 'EXIT_WEAK_MOMENTUM'
                confidence = 0.7
                del self.active_positions[symbol]
            else:
                recommendation = 'HOLD'
                confidence = pump_strength / 100

        return PumpSignal(
            symbol=symbol,
            timestamp=time.time(),
            price=price,
            volume_24h=current.get('volume_24h', vol_stats['total']),
            pump_strength=pump_strength,
            volume_surge_ratio=volume_surge,
            price_change_1m=price_change_1m,
            price_change_5m=price_change_5m,
            price_change_15m=price_change_15m,
            order_book_imbalance=ob_imbalance,
            whale_buy_pressure=whale_pressure,
            momentum_score=momentum,
            recommendation=recommendation,
            entry_price=entry_price,
            take_profit=take_profit,
            stop_loss=stop_loss,
            confidence=confidence
        )

    def get_active_positions_summary(self) -> List[dict]:
        """獲取所有持倉摘要"""
        summary = []
        for symbol, pos in self.active_positions.items():
            history = self.buffer.get_history(symbol, 1)
            current_price = history[-1]['price'] if history else pos['entry_price']
            pnl_pct = (current_price - pos['entry_price']) / pos['entry_price'] * 100
            summary.append({
                'symbol': symbol,
                'entry_price': pos['entry_price'],
                'current_price': current_price,
                'pnl_pct': round(pnl_pct, 2),
                'take_profit': pos['take_profit'],
                'stop_loss': pos['stop_loss'],
                'highest_price': pos['highest_price'],
                'hold_time_min': round((time.time() - pos['entry_time']) / 60, 1)
            })
        return summary
