"""
穩定 WebSocket 連線管理器
解決斷線問題：自動重連、心跳機制、指數退避、連線健康檢查
"""

import asyncio
import json
import time
import logging
import random
from typing import Dict, List, Optional, Callable, Set
from dataclasses import dataclass

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatusCode

logger = logging.getLogger(__name__)


@dataclass
class WSConfig:
    """WebSocket 配置"""
    url: str
    ping_interval: float = 20.0      # 心跳間隔（秒）
    pong_timeout: float = 10.0        # pong 超時
    reconnect_min: float = 1.0       # 最小重連間隔
    reconnect_max: float = 60.0       # 最大重連間隔
    reconnect_multiplier: float = 2.0 # 退避乘數
    max_reconnect_attempts: int = 0   # 0 = 無限重連
    connection_timeout: float = 30.0  # 連線超時


class StableWebSocket:
    """
    穩定 WebSocket 客戶端
    - 自動重連（指數退避）
    - 心跳保活
    - 連線健康監控
    - 訂閱狀態持久化
    """

    def __init__(self, config: WSConfig):
        self.config = config
        self.ws = None
        self.connected = False
        self.reconnect_count = 0
        self.last_pong_time = 0
        self.last_ping_time = 0
        self.subscriptions: Set[str] = set()
        self.message_handlers: List[Callable] = []
        self.error_handlers: List[Callable] = []
        self._stop_event = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._connection_stats = {
            'total_reconnects': 0,
            'total_messages': 0,
            'start_time': time.time(),
            'last_disconnect_reason': None
        }

    def on_message(self, handler: Callable):
        """註冊消息處理器"""
        self.message_handlers.append(handler)

    def on_error(self, handler: Callable):
        """註冊錯誤處理器"""
        self.error_handlers.append(handler)

    def _get_reconnect_delay(self) -> float:
        """計算重連延遲（指數退避 + 抖動）"""
        delay = self.config.reconnect_min * (self.config.reconnect_multiplier ** self.reconnect_count)
        delay = min(delay, self.config.reconnect_max)
        # 添加隨機抖動 (0-30%)
        delay *= (1 + random.random() * 0.3)
        return delay

    async def connect(self):
        """建立連線"""
        while not self._stop_event.is_set():
            try:
                logger.info(f"🔌 嘗試連線到 {self.config.url} (第 {self.reconnect_count + 1} 次)")

                self.ws = await asyncio.wait_for(
                    websockets.connect(
                        self.config.url,
                        ping_interval=None,  # 我們自己管理心跳
                        close_timeout=5,
                        max_size=10 * 1024 * 1024,  # 10MB
                    ),
                    timeout=self.config.connection_timeout
                )

                self.connected = True
                self.reconnect_count = 0
                self.last_pong_time = time.time()
                logger.info(f"✅ WebSocket 連線成功")

                # 重新訂閱之前的頻道
                if self.subscriptions:
                    await self._resubscribe()

                # 啟動心跳和接收任務
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
                self._receive_task = asyncio.create_task(self._receive_loop())

                # 等待任務結束（斷線時）
                done, pending = await asyncio.wait(
                    [self._heartbeat_task, self._receive_task],
                    return_when=asyncio.FIRST_COMPLETED
                )

                # 取消剩餘任務
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                # 檢查是否因為 stop_event 而結束
                if self._stop_event.is_set():
                    break

                self.connected = False
                self._connection_stats['total_reconnects'] += 1

            except asyncio.TimeoutError:
                logger.error("⏱️ 連線超時")
                self._connection_stats['last_disconnect_reason'] = 'timeout'
            except ConnectionRefusedError:
                logger.error("🚫 連線被拒絕")
                self._connection_stats['last_disconnect_reason'] = 'refused'
            except InvalidStatusCode as e:
                logger.error(f"🚫 無效狀態碼: {e.status_code}")
                self._connection_stats['last_disconnect_reason'] = f'status_{e.status_code}'
            except Exception as e:
                logger.error(f"❌ 連線錯誤: {type(e).__name__}: {e}")
                self._connection_stats['last_disconnect_reason'] = type(e).__name__

            self.connected = False

            # 檢查是否需要停止
            if self._stop_event.is_set():
                break

            # 檢查重連次數
            if (self.config.max_reconnect_attempts > 0 and 
                self.reconnect_count >= self.config.max_reconnect_attempts):
                logger.error(f"🚫 達到最大重連次數 {self.config.max_reconnect_attempts}")
                break

            self.reconnect_count += 1
            delay = self._get_reconnect_delay()
            logger.info(f"⏳ {delay:.1f} 秒後重連...")
            await asyncio.sleep(delay)

    async def _heartbeat_loop(self):
        """心跳循環"""
        try:
            while self.connected and not self._stop_event.is_set():
                await asyncio.sleep(self.config.ping_interval)

                if not self.ws or not self.connected:
                    break

                try:
                    # 發送 ping
                    ping_data = json.dumps({'ping': int(time.time() * 1000)})
                    await self.ws.send(ping_data)
                    self.last_ping_time = time.time()

                    # 檢查 pong 超時
                    await asyncio.sleep(self.config.pong_timeout)

                    if time.time() - self.last_pong_time > self.config.ping_interval + self.config.pong_timeout:
                        logger.warning("💔 心跳超時，斷線重連")
                        await self.ws.close()
                        break

                except Exception as e:
                    logger.error(f"心跳錯誤: {e}")
                    break

        except asyncio.CancelledError:
            pass

    async def _receive_loop(self):
        """接收消息循環"""
        try:
            async for message in self.ws:
                self._connection_stats['total_messages'] += 1

                try:
                    data = json.loads(message)

                    # 處理 pong
                    if 'pong' in data or 'pong' in str(data):
                        self.last_pong_time = time.time()
                        continue

                    # 處理訂閲確認
                    if 'event' in data and data['event'] == 'subscribe':
                        logger.info(f"📡 訂閲確認: {data}")
                        continue

                    # 分發給處理器
                    for handler in self.message_handlers:
                        try:
                            if asyncio.iscoroutinefunction(handler):
                                asyncio.create_task(handler(data))
                            else:
                                handler(data)
                        except Exception as e:
                            logger.error(f"消息處理器錯誤: {e}")

                except json.JSONDecodeError:
                    logger.warning(f"無法解析的消息: {message[:200]}")
                except Exception as e:
                    logger.error(f"處理消息錯誤: {e}")
                    for handler in self.error_handlers:
                        try:
                            handler(e)
                        except:
                            pass

        except ConnectionClosed as e:
            logger.warning(f"🔌 連線關閉: code={e.code}, reason={e.reason}")
            self._connection_stats['last_disconnect_reason'] = f'closed_{e.code}'
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"接收循環錯誤: {e}")

    async def _resubscribe(self):
        """重新訂閲所有頻道"""
        for channel in self.subscriptions:
            try:
                await self.subscribe(channel)
                await asyncio.sleep(0.1)  # 避免發送過快
            except Exception as e:
                logger.error(f"重新訂閲 {channel} 失敗: {e}")

    async def subscribe(self, channel: str):
        """訂閱頻道"""
        self.subscriptions.add(channel)
        if self.connected and self.ws:
            sub_msg = json.dumps({
                'op': 'subscribe',
                'args': [channel] if isinstance(channel, str) else channel
            })
            await self.ws.send(sub_msg)
            logger.info(f"📡 訂閲: {channel}")

    async def unsubscribe(self, channel: str):
        """取消訂閲"""
        self.subscriptions.discard(channel)
        if self.connected and self.ws:
            unsub_msg = json.dumps({
                'op': 'unsubscribe',
                'args': [channel] if isinstance(channel, str) else channel
            })
            await self.ws.send(unsub_msg)

    async def send(self, data: dict):
        """發送消息"""
        if self.connected and self.ws:
            await self.ws.send(json.dumps(data))

    async def close(self):
        """關閉連線"""
        logger.info("🛑 正在關閉 WebSocket...")
        self._stop_event.set()

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._receive_task:
            self._receive_task.cancel()

        if self.ws:
            await self.ws.close()

        self.connected = False
        logger.info("✅ WebSocket 已關閉")

    def get_stats(self) -> dict:
        """獲取連線統計"""
        uptime = time.time() - self._connection_stats['start_time']
        return {
            'connected': self.connected,
            'uptime_seconds': round(uptime, 0),
            'total_reconnects': self._connection_stats['total_reconnects'],
            'total_messages': self._connection_stats['total_messages'],
            'subscriptions': list(self.subscriptions),
            'last_disconnect': self._connection_stats['last_disconnect_reason']
        }


class ConnectionPool:
    """
    WebSocket 連線池
    為多個交易對分散連線，避免單一連線負載過重
    """

    def __init__(self, base_url: str, max_per_connection: int = 20):
        self.base_url = base_url
        self.max_per_connection = max_per_connection
        self.connections: List[StableWebSocket] = []
        self._symbol_to_connection: Dict[str, StableWebSocket] = {}

    async def subscribe_symbols(self, symbols: List[str], channel_prefix: str, handler: Callable):
        """訂閱多個交易對，自動分配到不同連線"""
        for i, symbol in enumerate(symbols):
            conn_index = i // self.max_per_connection

            # 確保有足夠的連線
            while len(self.connections) <= conn_index:
                config = WSConfig(url=self.base_url)
                conn = StableWebSocket(config)
                conn.on_message(handler)
                self.connections.append(conn)
                # 啟動連線
                asyncio.create_task(conn.connect())
                await asyncio.sleep(1)  # 等待連線建立

            conn = self.connections[conn_index]
            channel = f"{channel_prefix}:{symbol}"
            await conn.subscribe(channel)
            self._symbol_to_connection[symbol] = conn

    async def close_all(self):
        """關閉所有連線"""
        for conn in self.connections:
            await conn.close()
        self.connections.clear()
        self._symbol_to_connection.clear()
