"""
Telegram 通知模組
穩定發送、錯誤重試、消息格式化
"""

import asyncio
import logging
from typing import Optional
from datetime import datetime

import aiohttp

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Telegram 通知器"""

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.session: Optional[aiohttp.ClientSession] = None
        self._retry_count = 3
        self._retry_delay = 2

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self.session

    async def send_message(self, text: str, parse_mode: str = 'HTML', 
                         disable_notification: bool = False) -> bool:
        """發送消息（帶重試）"""
        url = f"{self.base_url}/sendMessage"
        payload = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': parse_mode,
            'disable_notification': disable_notification,
            'disable_web_page_preview': True
        }

        for attempt in range(self._retry_count):
            try:
                session = await self._get_session()
                async with session.post(url, json=payload) as resp:
                    result = await resp.json()
                    if result.get('ok'):
                        return True
                    else:
                        logger.warning(f"Telegram API 錯誤: {result}")

            except Exception as e:
                logger.error(f"發送 Telegram 消息失敗 (嘗試 {attempt + 1}/{self._retry_count}): {e}")
                if attempt < self._retry_count - 1:
                    await asyncio.sleep(self._retry_delay * (attempt + 1))

        return False

    async def send_pump_alert(self, signal: dict) -> bool:
        """發送拉盤警報"""
        emoji_map = {
            'BUY': '🚀',
            'HOLD': '⏳',
            'SELL': '🔴',
            'WAIT': '⏸️',
            'TAKE_PROFIT': '💰',
            'STOP_LOSS': '🛑',
            'EXIT_WEAK_MOMENTUM': '⚠️'
        }

        rec = signal['recommendation']
        emoji = emoji_map.get(rec, '📊')

        # 根據拉盤力度選擇顏色
        strength = signal['pump_strength']
        if strength >= 80:
            strength_bar = '🔥🔥🔥🔥🔥'
        elif strength >= 60:
            strength_bar = '🔥🔥🔥🔥'
        elif strength >= 40:
            strength_bar = '🔥🔥🔥'
        elif strength >= 20:
            strength_bar = '🔥🔥'
        else:
            strength_bar = '🔥'

        text = f"""
{emoji} <b>WEEX 拉盤警報</b> {emoji}

<b>幣種:</b> <code>{signal['symbol']}</code>
<b>當前價格:</b> ${signal['price']:.6f}
<b>時間:</b> {signal['time_str']}

<b>拉盤力度:</b> {signal['pump_strength']:.1f}/100 {strength_bar}
<b>信心度:</b> {signal['confidence']*100:.0f}%

📊 <b>關鍵指標:</b>
• 量比: {signal['volume_surge_ratio']:.2f}x
• 1分鐘漲幅: {signal['price_change_1m']*100:.2f}%
• 5分鐘漲幅: {signal['price_change_5m']*100:.2f}%
• 15分鐘漲幅: {signal['price_change_15m']*100:.2f}%
• 買賣盤失衡: {signal['order_book_imbalance']:.3f}
• 大單買壓: {signal['whale_buy_pressure']:.2f}
• 動能分數: {signal['momentum_score']:.1f}

<b>建議操作:</b> <code>{rec}</code>
"""

        if signal['entry_price']:
            text += f"""
💡 <b>交易建議:</b>
• 進場價: ${signal['entry_price']:.6f}
• 止盈價: ${signal['take_profit']:.6f} (+{((signal['take_profit']/signal['entry_price'])-1)*100:.1f}%)
• 止損價: ${signal['stop_loss']:.6f} ({((signal['stop_loss']/signal['entry_price'])-1)*100:.1f}%)
"""

        text += f"
<i>⚠️ 此為自動分析，僅供參考，投資需謹慎</i>"

        return await self.send_message(text)

    async def send_position_update(self, positions: list) -> bool:
        """發送持倉更新"""
        if not positions:
            return await self.send_message("📭 <b>當前無持倉</b>")

        text = "📊 <b>持倉監控</b>

"

        for pos in positions:
            pnl_emoji = '🟢' if pos['pnl_pct'] >= 0 else '🔴'
            text += f"""
{pnl_emoji} <b>{pos['symbol']}</b>
• 進場: ${pos['entry_price']:.6f}
• 現價: ${pos['current_price']:.6f}
• 盈虧: {pos['pnl_pct']:+.2f}%
• 持倉: {pos['hold_time_min']:.0f} 分鐘
• 止盈: ${pos['take_profit']:.6f}
• 止損: ${pos['stop_loss']:.6f}
• 最高: ${pos['highest_price']:.6f}
"""

        return await self.send_message(text)

    async def send_status_report(self, stats: dict) -> bool:
        """發送狀態報告"""
        text = f"""
📡 <b>系統狀態報告</b>

<b>運行時間:</b> {stats.get('uptime', 'N/A')}
<b>監控幣種:</b> {stats.get('symbols_count', 0)} 個
<b>活躍持倉:</b> {stats.get('active_positions', 0)} 個
<b>今日信號:</b> {stats.get('signals_today', 0)} 個
<b>WebSocket 狀態:</b> {'🟢 正常' if stats.get('ws_connected') else '🔴 斷線'}
<b>重連次數:</b> {stats.get('reconnects', 0)}

<i>系統正常運行中 🟢</i>
"""
        return await self.send_message(text)

    async def send_startup_notification(self, symbols: list) -> bool:
        """發送啟動通知"""
        text = f"""
🚀 <b>WEEX 拉盤監控系統已啟動</b>

<b>監控幣種數量:</b> {len(symbols)} 個
<b>啟動時間:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

<b>監控列表:</b>
<code>{', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}</code>

<i>系統將持續監控拉盤信號並即時通知</i>
"""
        return await self.send_message(text)

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
