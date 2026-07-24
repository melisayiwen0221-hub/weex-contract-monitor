"""
WEEX API 適配器
支援 REST + WebSocket V3
"""

import asyncio
import json
import time
import hmac
import hashlib
import base64
import logging
from typing import Dict, List, Optional
from urllib.parse import urlencode

import aiohttp

logger = logging.getLogger(__name__)


class WeexAPI:
    """WEEX API 客戶端"""

    REST_BASE = "https://api.weex.com"
    WS_PUBLIC = "wss://wspool.weex.com/ws/v3?appid=weex"

    def __init__(self, api_key: Optional[str] = None, 
                 api_secret: Optional[str] = None,
                 passphrase: Optional[str] = None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={'Content-Type': 'application/json'}
            )
        return self.session

    def _generate_signature(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        """生成 API 簽名"""
        message = timestamp + method.upper() + path + body
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        return signature

    async def _request(self, method: str, path: str, params: Optional[dict] = None, 
                       body: Optional[dict] = None, auth: bool = False) -> dict:
        """發送 HTTP 請求"""
        session = await self._get_session()

        url = f"{self.REST_BASE}{path}"
        headers = {}

        if auth and self.api_key:
            timestamp = str(int(time.time() * 1000))
            body_str = json.dumps(body) if body else ""
            signature = self._generate_signature(timestamp, method, path, body_str)
            headers = {
                'ACCESS-KEY': self.api_key,
                'ACCESS-SIGN': signature,
                'ACCESS-TIMESTAMP': timestamp,
                'ACCESS-PASSPHRASE': self.passphrase or '',
                'Content-Type': 'application/json'
            }

        try:
            if method.upper() == 'GET':
                async with session.get(url, params=params, headers=headers) as resp:
                    return await resp.json()
            else:
                async with session.post(url, json=body, headers=headers) as resp:
                    return await resp.json()
        except Exception as e:
            logger.error(f"API 請求失敗 {path}: {e}")
            return {'code': 'error', 'msg': str(e)}

    async def get_server_time(self) -> dict:
        """獲取伺服器時間"""
        return await self._request('GET', '/api/spot/v1/public/time')

    async def get_all_tickers(self) -> List[dict]:
        """獲取所有交易對行情"""
        resp = await self._request('GET', '/api/spot/v1/market/tickers')
        if resp.get('code') == '00000':
            return resp.get('data', [])
        logger.warning(f"獲取行情失敗: {resp}")
        return []

    async def get_ticker(self, symbol: str) -> dict:
        """獲取單個交易對行情"""
        resp = await self._request('GET', f'/api/spot/v1/market/ticker?symbol={symbol}')
        if resp.get('code') == '00000':
            return resp.get('data', {})
        return {}

    async def get_order_book(self, symbol: str, limit: int = 20) -> dict:
        """獲取訂單簿"""
        resp = await self._request('GET', f'/api/spot/v1/market/depth?symbol={symbol}&limit={limit}')
        if resp.get('code') == '00000':
            return resp.get('data', {})
        return {}

    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[dict]:
        """獲取近期成交"""
        resp = await self._request('GET', f'/api/spot/v1/market/fills?symbol={symbol}&limit={limit}')
        if resp.get('code') == '00000':
            return resp.get('data', [])
        return []

    async def get_klines(self, symbol: str, period: str = '1m', limit: int = 100) -> List[dict]:
        """獲取K線數據"""
        resp = await self._request('GET', 
            f'/api/spot/v1/market/candles?symbol={symbol}&period={period}&limit={limit}')
        if resp.get('code') == '00000':
            return resp.get('data', [])
        return []

    async def get_spot_symbols(self) -> List[str]:
        """獲取所有現貨交易對"""
        resp = await self._request('GET', '/api/spot/v1/public/products')
        if resp.get('code') == '00000':
            products = resp.get('data', [])
            return [p['symbol'] for p in products if p.get('status') == 'online']
        return []

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()


class WeexWebSocketAdapter:
    """
    WEEX WebSocket 消息適配器
    將 WEEX 的 WebSocket 消息格式轉換為統一格式
    """

    @staticmethod
    def parse_ticker(data: dict) -> Optional[dict]:
        """解析 ticker 消息"""
        if 'data' not in data:
            return None

        ticker_data = data.get('data', {})
        if isinstance(ticker_data, list) and len(ticker_data) > 0:
            ticker_data = ticker_data[0]

        return {
            'symbol': ticker_data.get('instId', ticker_data.get('symbol', '')),
            'last': float(ticker_data.get('last', 0)),
            'open': float(ticker_data.get('open24h', ticker_data.get('open', 0))),
            'high': float(ticker_data.get('high24h', ticker_data.get('high', 0))),
            'low': float(ticker_data.get('low24h', ticker_data.get('low', 0))),
            'volume': float(ticker_data.get('baseVolume', ticker_data.get('vol', 0))),
            'quote_volume': float(ticker_data.get('quoteVolume', 0)),
            'timestamp': time.time()
        }

    @staticmethod
    def parse_order_book(data: dict) -> Optional[dict]:
        """解析訂單簿消息"""
        if 'data' not in data:
            return None

        ob_data = data.get('data', {})
        if isinstance(ob_data, list) and len(ob_data) > 0:
            ob_data = ob_data[0]

        return {
            'symbol': ob_data.get('instId', ''),
            'bids': ob_data.get('bids', []),
            'asks': ob_data.get('asks', []),
            'timestamp': time.time()
        }

    @staticmethod
    def parse_trade(data: dict) -> Optional[List[dict]]:
        """解析成交消息"""
        if 'data' not in data:
            return None

        trades_data = data.get('data', [])
        if not isinstance(trades_data, list):
            trades_data = [trades_data]

        trades = []
        for t in trades_data:
            trades.append({
                'symbol': t.get('instId', ''),
                'price': float(t.get('price', 0)),
                'size': float(t.get('size', t.get('qty', 0))),
                'side': 'buy' if t.get('side') == 'buy' else 'sell',
                'timestamp': float(t.get('ts', time.time())) / 1000
            })
        return trades

    @staticmethod
    def create_subscribe_message(channels: List[str]) -> dict:
        """創建訂閱消息"""
        return {
            "op": "subscribe",
            "args": [{"channel": ch} for ch in channels]
        }
