"""
WEEX 山寨幣拉盤監控系統 - 主程式
運行穩定、自動重連、即時偵測、Telegram 通知
"""

import asyncio
import json
import os
import time
import logging
import signal
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from collections import defaultdict

from dotenv import load_dotenv

from src.websocket_manager import StableWebSocket, WSConfig, ConnectionPool
from src.weex_api import WeexAPI, WeexWebSocketAdapter
from src.pump_detector import PumpDetector, PumpSignal
from src.telegram_notifier import TelegramNotifier

# 載入環境變數
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('tracker.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class WeexPumpTracker:
    """
    WEEX 拉盤監控系統主類
    """

    def __init__(self):
        # 配置
        self.symbols: List[str] = []
        self.max_symbols = 30

        # API 客戶端
        self.api = WeexAPI(
            api_key=os.getenv('WEEX_API_KEY'),
            api_secret=os.getenv('WEEX_API_SECRET'),
            passphrase=os.getenv('WEEX_PASSPHRASE')
        )

        # Telegram
        self.telegram = TelegramNotifier(
            bot_token=os.getenv('TELEGRAM_BOT_TOKEN', ''),
            chat_id=os.getenv('TELEGRAM_CHAT_ID', '')
        )

        # 拉盤偵測器
        self.detector = PumpDetector()

        # WebSocket
        self.ws_connections: List[StableWebSocket] = []
        self.ws_adapter = WeexWebSocketAdapter()

        # 運行狀態
        self.running = False
        self._stop_event = asyncio.Event()
        self._tasks: List[asyncio.Task] = []

        # 統計
        self.stats = {
            'start_time': time.time(),
            'signals_today': 0,
            'last_signal_time': 0,
            'ws_connected': False,
            'reconnects': 0,
            'errors': 0
        }

        # REST 輪詢間隔（WebSocket 斷線時備用）
        self.rest_poll_interval = 3  # 秒

        # 信號冷卻（同一幣種 5 分鐘內不重複發送相同信號）
        self.signal_cooldown: Dict[str, float] = {}
        self.cooldown_seconds = 300

    async def initialize(self):
        """初始化系統"""
        logger.info("🚀 初始化 WEEX 拉盤監控系統...")

        # 1. 獲取交易對列表
        await self._load_symbols()

        # 2. 發送啟動通知
        if self.telegram.bot_token:
            await self.telegram.send_startup_notification(self.symbols)

        logger.info(f"✅ 初始化完成，監控 {len(self.symbols)} 個幣種")

    async def _load_symbols(self):
        """載入要監控的交易對"""
        # 優先從環境變數讀取
        env_symbols = os.getenv('TRACK_SYMBOLS', '')
        if env_symbols:
            self.symbols = [s.strip().upper() for s in env_symbols.split(',')]
            logger.info(f"從環境變數載入 {len(self.symbols)} 個幣種")
            return

        # 否則從 API 獲取熱門山寨幣
        try:
            all_tickers = await self.api.get_all_tickers()

            # 篩選條件：
            # 1. USDT 交易對
            # 2. 24h 成交量 > 100萬 USDT
            # 3. 排除 BTC、ETH 等主流幣
            exclude = {'BTCUSDT', 'ETHUSDT', 'USDCUSDT'}

            candidates = []
            for t in all_tickers:
                symbol = t.get('symbol', '')
                if not symbol.endswith('USDT'):
                    continue
                if symbol in exclude:
                    continue

                vol = float(t.get('quoteVolume', 0))
                if vol < 1_000_000:  # 100萬 USDT
                    continue

                candidates.append({
                    'symbol': symbol,
                    'volume': vol,
                    'price': float(t.get('last', 0))
                })

            # 按成交量排序，取前 30
            candidates.sort(key=lambda x: x['volume'], reverse=True)
            self.symbols = [c['symbol'] for c in candidates[:self.max_symbols]]

            logger.info(f"從 API 自動選取 {len(self.symbols)} 個熱門山寨幣")

        except Exception as e:
            logger.error(f"獲取交易對失敗: {e}")
            # 使用預設列表
            self.symbols = [
                'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'TRXUSDT',
                'AVAXUSDT', 'LINKUSDT', 'DOTUSDT', 'MATICUSDT', 'LTCUSDT',
                'BCHUSDT', 'UNIUSDT', 'ETCUSDT', 'XLMUSDT', 'FILUSDT',
                'ARBUSDT', 'OPUSDT', 'NEARUSDT', 'APTUSDT', 'SUIUSDT',
                'PEPEUSDT', 'SHIBUSDT', 'WIFUSDT', 'BONKUSDT', 'FLOKIUSDT',
                'WLDUSDT', 'SEIUSDT', 'TIAUSDT', 'STRKUSDT', 'PYTHUSDT'
            ]
            logger.info(f"使用預設列表: {len(self.symbols)} 個幣種")

    async def start(self):
        """啟動監控"""
        self.running = True

        # 註冊信號處理
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                asyncio.get_event_loop().add_signal_handler(
                    sig, lambda: asyncio.create_task(self.stop())
                )
            except NotImplementedError:
                pass  # Windows 不支援

        # 啟動多個並行任務
        self._tasks = [
            asyncio.create_task(self._websocket_monitor()),
            asyncio.create_task(self._rest_fallback_monitor()),
            asyncio.create_task(self._position_monitor()),
            asyncio.create_task(self._status_reporter()),
        ]

        logger.info("🟢 監控系統已啟動")

        # 等待停止信號
        await self._stop_event.wait()

        # 清理
        await self._cleanup()

    async def _websocket_monitor(self):
        """
        WebSocket 監控主循環
        使用穩定連線管理器，自動重連
        """
        while self.running and not self._stop_event.is_set():
            try:
                # 建立 WebSocket 連線
                config = WSConfig(
                    url=WeexAPI.WS_PUBLIC,
                    ping_interval=20,
                    pong_timeout=10,
                    reconnect_min=1,
                    reconnect_max=30
                )

                ws = StableWebSocket(config)

                # 註冊消息處理器
                ws.on_message(self._handle_ws_message)
                ws.on_error(self._handle_ws_error)

                # 訂閱所有幣種
                channels = []
                for symbol in self.symbols:
                    channels.extend([
                        f"tickers:{symbol}",
                        f"books:{symbol}",
                        f"trades:{symbol}"
                    ])

                # 分批訂閱（避免消息過大）
                batch_size = 10
                for i in range(0, len(channels), batch_size):
                    batch = channels[i:i+batch_size]
                    for ch in batch:
                        ws.subscriptions.add(ch)

                self.ws_connections = [ws]

                # 啟動連線（阻塞直到斷線）
                await ws.connect()

                self.stats['ws_connected'] = False
                self.stats['reconnects'] += 1

                # 如果不是主動停止，等待後重連
                if self.running and not self._stop_event.is_set():
                    logger.warning("WebSocket 斷線，等待重連...")
                    await asyncio.sleep(5)

            except Exception as e:
                logger.error(f"WebSocket 監控錯誤: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(10)

    def _handle_ws_message(self, data: dict):
        """處理 WebSocket 消息"""
        try:
            self.stats['ws_connected'] = True

            # 解析 ticker
            ticker = self.ws_adapter.parse_ticker(data)
            if ticker:
                symbol = ticker['symbol']
                self.detector.update_ticker(symbol, ticker)

                # 偵測拉盤
                signal = self.detector.detect_pump(symbol)
                if signal:
                    asyncio.create_task(self._process_signal(signal))
                return

            # 解析訂單簿
            ob = self.ws_adapter.parse_order_book(data)
            if ob:
                self.detector.update_order_book(
                    ob['symbol'], 
                    ob.get('bids', []), 
                    ob.get('asks', [])
                )
                return

            # 解析成交
            trades = self.ws_adapter.parse_trade(data)
            if trades:
                for trade in trades:
                    self.detector.update_trades(trade['symbol'], [trade])
                return

        except Exception as e:
            logger.error(f"處理 WebSocket 消息錯誤: {e}")

    def _handle_ws_error(self, error: Exception):
        """處理 WebSocket 錯誤"""
        logger.error(f"WebSocket 錯誤: {error}")
        self.stats['errors'] += 1

    async def _rest_fallback_monitor(self):
        """
        REST API 備用監控
        當 WebSocket 不穩定時，使用 REST API 輪詢作為備用
        """
        while self.running and not self._stop_event.is_set():
            try:
                # 如果 WebSocket 連線正常，減少 REST 請求頻率
                if self.stats.get('ws_connected'):
                    await asyncio.sleep(self.rest_poll_interval * 5)
                    continue

                logger.info("📡 使用 REST API 備用輪詢...")

                # 分批獲取行情（避免速率限制）
                for symbol in self.symbols:
                    try:
                        ticker = await self.api.get_ticker(symbol)
                        if ticker:
                            self.detector.update_ticker(symbol, {
                                'symbol': symbol,
                                'last': float(ticker.get('last', 0)),
                                'open': float(ticker.get('open24h', 0)),
                                'high': float(ticker.get('high24h', 0)),
                                'low': float(ticker.get('low24h', 0)),
                                'volume': float(ticker.get('baseVolume', 0)),
                                'timestamp': time.time()
                            })

                            # 獲取訂單簿
                            ob = await self.api.get_order_book(symbol, 10)
                            if ob:
                                self.detector.update_order_book(
                                    symbol,
                                    ob.get('bids', []),
                                    ob.get('asks', [])
                                )

                            # 偵測拉盤
                            signal = self.detector.detect_pump(symbol)
                            if signal:
                                await self._process_signal(signal)

                        # 速率限制：每秒最多 2 個請求
                        await asyncio.sleep(0.5)

                    except Exception as e:
                        logger.error(f"輪詢 {symbol} 失敗: {e}")

                await asyncio.sleep(self.rest_poll_interval)

            except Exception as e:
                logger.error(f"REST 備用監控錯誤: {e}")
                await asyncio.sleep(10)

    async def _process_signal(self, signal: PumpSignal):
        """處理拉盤信號"""
        try:
            symbol = signal.symbol
            rec = signal.recommendation

            # 檢查冷卻
            now = time.time()
            if symbol in self.signal_cooldown:
                if now - self.signal_cooldown[symbol] < self.cooldown_seconds:
                    if rec not in ['TAKE_PROFIT', 'STOP_LOSS', 'EXIT_WEAK_MOMENTUM']:
                        return  # 冷卻中，忽略非強制信號

            # 更新冷卻時間
            self.signal_cooldown[symbol] = now

            # 更新統計
            self.stats['signals_today'] += 1
            self.stats['last_signal_time'] = now

            # 記錄日誌
            logger.info(f"🚨 拉盤信號: {symbol} | 力度: {signal.pump_strength:.1f} | 建議: {rec}")

            # 發送 Telegram 通知
            if self.telegram.bot_token:
                await self.telegram.send_pump_alert(signal.to_dict())

            # 保存到文件
            await self._save_signal(signal)

        except Exception as e:
            logger.error(f"處理信號錯誤: {e}")

    async def _save_signal(self, signal: PumpSignal):
        """保存信號到文件"""
        try:
            data = signal.to_dict()
            filename = f"signals_{datetime.now().strftime('%Y%m%d')}.jsonl"

            with open(filename, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data, ensure_ascii=False) + '\n')

        except Exception as e:
            logger.error(f"保存信號失敗: {e}")

    async def _position_monitor(self):
        """持倉監控循環"""
        while self.running and not self._stop_event.is_set():
            try:
                await asyncio.sleep(30)  # 每 30 秒檢查一次

                positions = self.detector.get_active_positions_summary()
                if positions:
                    logger.info(f"📊 當前持倉: {len(positions)} 個")

                    # 檢查是否需要通知
                    for pos in positions:
                        symbol = pos['symbol']
                        pnl = pos['pnl_pct']

                        # 大幅盈虧通知
                        if pnl >= 10 or pnl <= -3:
                            if self.telegram.bot_token:
                                emoji = '🟢' if pnl >= 0 else '🔴'
                                text = f"""
{emoji} <b>持倉異動提醒</b>

<b>{symbol}</b>
• 盈虧: {pnl:+.2f}%
• 現價: ${pos['current_price']:.6f}
• 持倉: {pos['hold_time_min']:.0f} 分鐘
"""
                                await self.telegram.send_message(text)

                # 發送持倉更新（每 5 分鐘）
                if int(time.time()) % 300 < 35 and positions:
                    if self.telegram.bot_token:
                        await self.telegram.send_position_update(positions)

            except Exception as e:
                logger.error(f"持倉監控錯誤: {e}")

    async def _status_reporter(self):
        """狀態報告循環"""
        while self.running and not self._stop_event.is_set():
            try:
                await asyncio.sleep(3600)  # 每小時報告

                uptime = timedelta(seconds=int(time.time() - self.stats['start_time']))

                stats = {
                    'uptime': str(uptime),
                    'symbols_count': len(self.symbols),
                    'active_positions': len(self.detector.active_positions),
                    'signals_today': self.stats['signals_today'],
                    'ws_connected': self.stats['ws_connected'],
                    'reconnects': self.stats['reconnects']
                }

                logger.info(f"📊 狀態報告: {stats}")

                if self.telegram.bot_token:
                    await self.telegram.send_status_report(stats)

            except Exception as e:
                logger.error(f"狀態報告錯誤: {e}")

    async def stop(self):
        """停止監控"""
        logger.info("🛑 正在停止監控系統...")
        self.running = False
        self._stop_event.set()

        # 取消所有任務
        for task in self._tasks:
            task.cancel()

        await self._cleanup()
        logger.info("✅ 監控系統已停止")

    async def _cleanup(self):
        """清理資源"""
        # 關閉 WebSocket
        for ws in self.ws_connections:
            await ws.close()

        # 關閉 API session
        await self.api.close()

        # 關閉 Telegram
        await self.telegram.close()


async def main():
    """主入口"""
    tracker = WeexPumpTracker()

    try:
        await tracker.initialize()
        await tracker.start()
    except Exception as e:
        logger.error(f"系統錯誤: {e}")
        raise


if __name__ == '__main__':
    asyncio.run(main())
