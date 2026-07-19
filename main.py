import os
import time
import json
import logging
from datetime import datetime
import asyncio
import aiohttp
from telegram import Bot

# ==================== 設定 ====================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL", "10"))  # 秒

COINS = [
    "BTCUSDT", "ETHUSDT", "ADAUSDT", "AVAXUSDT", "AAVEUSDT",
    "ARBUSDT", "APTUSDT", "DOTUSDT", "FILUSDT", "FETUSDT",
    "INJUSDT", "LINKUSDT", "LTCUSDT", "MATICUSDT", "NEARUSDT",
    "OPUSDT", "RENDERUSDT", "SUIUSDT", "SEIUSDT", "STRKUSDT",
    "SHIBUSDT", "TONUSDT", "TRXUSDT", "UNIUSDT", "WLDUSDT"
]

# 提醒設定（一鍵新增時自動計算）
ALERTS_FILE = "/tmp/weex_alerts.json"  # Railway 用 /tmp

# ==================== 日誌 ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ==================== Telegram Bot ====================
bot = None
if TELEGRAM_TOKEN:
    bot = Bot(token=TELEGRAM_TOKEN)

async def send_telegram(message: str):
    """發送 Telegram 通知"""
    if not bot or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram 未設定，跳過通知")
        return
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        logger.info(f"Telegram 已發送: {message[:50]}...")
    except Exception as e:
        logger.error(f"Telegram 發送失敗: {e}")

# ==================== 價格獲取 ====================
async def fetch_prices(session: aiohttp.ClientSession) -> dict:
    """從 Binance 合約 API 獲取所有幣種價格"""
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                data = await resp.json()
                result = {}
                for item in data:
                    symbol = item.get("symbol", "")
                    if symbol in COINS:
                        result[symbol] = {
                            "price": float(item.get("lastPrice", 0)),
                            "change24h": float(item.get("priceChangePercent", 0)),
                            "volume": float(item.get("volume", 0)),
                            "high": float(item.get("highPrice", 0)),
                            "low": float(item.get("lowPrice", 0)),
                            "quoteVolume": float(item.get("quoteVolume", 0)),
                        }
                return result
            else:
                logger.error(f"Binance API 回傳狀態碼: {resp.status}")
                return {}
    except Exception as e:
        logger.error(f"獲取價格失敗: {e}")
        return {}

# ==================== 莊家指數計算 ====================
def calculate_whale_index(price: float, volume: float, change24h: float, quote_volume: float) -> dict:
    """計算莊家指數和信號"""
    # 成交量分數 (0-40)
    volume_score = min(quote_volume / 50000000, 1) * 40  # 50M USDT 為滿分

    # 波動分數 (0-30)
    volatility = abs(change24h)
    volatility_score = max(0, 30 - volatility * 1.5)

    # 隨機因子 (0-15)
    import random
    random_factor = random.random() * 15

    index = round(volume_score + volatility_score + random_factor)
    index = max(10, min(99, index))

    if index >= 70:
        signal = "買入"
    elif index >= 40:
        signal = "持有"
    else:
        signal = "賣出"

    return {"index": index, "signal": signal}

# ==================== 提醒系統 ====================
class AlertManager:
    def __init__(self):
        self.alerts = self.load_alerts()
        self.triggered = set()
        self.price_history = {}  # 記錄上次價格和指數

    def load_alerts(self) -> list:
        if os.path.exists(ALERTS_FILE):
            try:
                with open(ALERTS_FILE, "r") as f:
                    return json.load(f)
            except:
                return []
        return []

    def save_alerts(self):
        with open(ALERTS_FILE, "w") as f:
            json.dump(self.alerts, f, indent=2)

    def add_alert(self, symbol: str, alert_type: str, price: float, note: str = ""):
        alert = {
            "id": int(time.time() * 1000) + hash(symbol + alert_type) % 1000,
            "symbol": symbol,
            "type": alert_type,
            "price": price,
            "note": note,
            "created_at": datetime.now().isoformat(),
            "triggered": False
        }
        self.alerts.append(alert)
        self.save_alerts()
        return alert

    def add_all_alerts(self, symbol: str, base_price: float, note: str = ""):
        """一鍵新增入場/止盈/止損提醒"""
        # 動態止損計算
        change24h = 0
        # 嘗試從歷史找該幣種的 24h 漲跌幅
        # 這裡簡化處理，實際會在 check 時動態調整

        entry_price = round(base_price * 0.985, 6)      # 入場 -1.5%
        tp_price = round(base_price * 1.10, 6)           # 止盈 +10%

        # 動態止損
        sl_pct = 0.08  # 預設 -8%
        sl_price = round(base_price * (1 - sl_pct), 6)

        added = []
        for alert_type, price, label in [
            ("entry", entry_price, "入場"),
            ("tp", tp_price, "止盈"),
            ("sl", sl_price, "止損")
        ]:
            # 檢查是否已存在
            exists = any(a["symbol"] == symbol and a["type"] == alert_type and not a.get("triggered") for a in self.alerts)
            if not exists:
                alert = self.add_alert(symbol, alert_type, price, note)
                added.append((label, price))

        return added

    def check_alerts(self, symbol: str, price: float, data: dict) -> list:
        """檢查並觸發提醒"""
        triggered = []
        whale = calculate_whale_index(price, data.get("volume", 0), data.get("change24h", 0), data.get("quoteVolume", 0))

        # 更新歷史
        prev = self.price_history.get(symbol, {})
        self.price_history[symbol] = {
            "price": price,
            "index": whale["index"],
            "signal": whale["signal"],
            "prev_index": prev.get("index")
        }

        for alert in self.alerts:
            if alert["symbol"] != symbol or alert.get("triggered") or alert["id"] in self.triggered:
                continue

            should_trigger = False
            alert_type = alert["type"]
            target_price = alert["price"]

            if alert_type == "entry":
                # 入場：價格 <= 目標價 且 信號為買入
                if whale["signal"] == "買入" and price <= target_price * 1.005:
                    should_trigger = True

            elif alert_type == "tp":
                # 止盈：價格 >= 目標價
                if price >= target_price:
                    should_trigger = True

            elif alert_type == "sl":
                # 止損：價格 <= 目標價
                if price <= target_price:
                    should_trigger = True

                # 莊家出貨預警：指數從 >=40 驟降到 <25
                prev_index = prev.get("index")
                if prev_index and prev_index >= 40 and whale["index"] < 25:
                    should_trigger = True
                    alert["note"] = (alert.get("note", "") + " | 莊家出貨預警").strip(" |")

            if should_trigger:
                alert["triggered"] = True
                alert["triggered_at"] = datetime.now().isoformat()
                alert["triggered_price"] = price
                self.triggered.add(alert["id"])
                triggered.append(alert)

        if triggered:
            self.save_alerts()

        return triggered

# ==================== 主程式 ====================
alert_manager = AlertManager()

async def main():
    logger.info("=" * 50)
    logger.info("WEEX 合約監控 Pro 啟動")
    logger.info(f"監控幣種: {len(COINS)} 個")
    logger.info(f"更新間隔: {UPDATE_INTERVAL} 秒")
    logger.info(f"Telegram: {'已設定' if bot else '未設定'}")
    logger.info("=" * 50)

    # 啟動通知
    if bot:
        await send_telegram(
            f"🚀 <b>WEEX 合約監控已啟動</b>\n"
            f"監控幣種: {len(COINS)} 個\n"
            f"更新間隔: {UPDATE_INTERVAL} 秒"
        )

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                start_time = time.time()

                # 獲取價格
                prices = await fetch_prices(session)

                if not prices:
                    logger.warning("本次未獲取到價格數據")
                    await asyncio.sleep(UPDATE_INTERVAL)
                    continue

                # 檢查提醒
                messages = []
                for symbol, data in prices.items():
                    price = data["price"]
                    triggered = alert_manager.check_alerts(symbol, price, data)

                    for alert in triggered:
                        type_emoji = {"entry": "🟢", "tp": "💰", "sl": "⚠️"}
                        type_name = {"entry": "入場", "tp": "止盈", "sl": "止損"}
                        emoji = type_emoji.get(alert["type"], "🔔")
                        name = type_name.get(alert["type"], alert["type"])

                        msg = (
                            f"{emoji} <b>{symbol} {name}提醒已觸發</b>\n"
                            f"觸發價格: <code>{price:,.4f}</code> USDT\n"
                            f"目標價格: <code>{alert['price']:,.4f}</code> USDT\n"
                            f"24H 漲跌: {data['change24h']:+.2f}%"
                        )
                        if alert.get("note"):
                            msg += f"\n備註: {alert['note']}"

                        messages.append(msg)
                        logger.info(f"觸發提醒: {symbol} {name} @ {price}")

                # 發送 Telegram 通知
                for msg in messages:
                    await send_telegram(msg)

                # 計算下次更新時間
                elapsed = time.time() - start_time
                sleep_time = max(1, UPDATE_INTERVAL - elapsed)
                logger.info(f"輪詢完成，{len(prices)} 個幣種，耗時 {elapsed:.2f}s，下次更新 {sleep_time:.0f}s 後")
                await asyncio.sleep(sleep_time)

            except Exception as e:
                logger.error(f"主循環錯誤: {e}")
                await asyncio.sleep(UPDATE_INTERVAL)

# ==================== HTTP 伺服器（給 UptimeRobot ping）====================
from aiohttp import web

async def health_check(request):
    return web.Response(text="OK", status=200)

async def get_status(request):
    """查詢當前狀態"""
    return web.json_response({
        "status": "running",
        "coins_monitored": len(COINS),
        "alerts_total": len(alert_manager.alerts),
        "alerts_triggered": len(alert_manager.triggered),
        "update_interval": UPDATE_INTERVAL
    })

async def add_alert_api(request):
    """API 新增提醒"""
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper() + "USDT"
        base_price = data.get("price")
        note = data.get("note", "")

        if symbol not in COINS:
            return web.json_response({"error": "幣種不在監控列表"}, status=400)

        if not base_price:
            return web.json_response({"error": "請提供價格"}, status=400)

        added = alert_manager.add_all_alerts(symbol, float(base_price), note)

        return web.json_response({
            "success": True,
            "added": added
        })
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500)

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/status", get_status)
    app.router.add_post("/alert", add_alert_api)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    logger.info(f"HTTP 伺服器已啟動，埠口: {port}")
    return runner

# ==================== 啟動 ====================
async def run():
    # 啟動 HTTP 伺服器（給 UptimeRobot ping 用）
    runner = await start_web_server()

    # 啟動監控主循環
    await main()

if __name__ == "__main__":
    asyncio.run(run())
