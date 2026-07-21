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
    "BTCUSDT", "ETHUSDT",
    "ADAUSDT", "AVAXUSDT", "AAVEUSDT", "ARBUSDT", "APTUSDT",
    "DOTUSDT", "FILUSDT", "FETUSDT", "INJUSDT", "LINKUSDT",
    "LTCUSDT", "MATICUSDT", "NEARUSDT", "OPUSDT", "RENDERUSDT",
    "SUIUSDT", "SEIUSDT", "STRKUSDT", "SHIBUSDT", "TONUSDT",
    "TRXUSDT", "UNIUSDT", "WLDUSDT",
    "XRPUSDT", "DOGEUSDT", "SOLUSDT", "BNBUSDT", "PEPEUSDT",
    "JTOUSDT", "ACEUSDT", "BIGTIMEUSDT", "BOMEUSDT", "WIFUSDT",
    "FLOKIUSDT", "BONKUSDT", "JUPUSDT", "PYTHUSDT", "IMXUSDT",
    "GRTUSDT", "LDOUSDT", "TIAUSDT", "SAGAUSDT", "ENAUSDT",
    "WUSDT"
]

# CoinGecko ID 對照表
COINGECKO_IDS = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "ADAUSDT": "cardano",
    "AVAXUSDT": "avalanche-2",
    "AAVEUSDT": "aave",
    "ARBUSDT": "arbitrum",
    "APTUSDT": "aptos",
    "DOTUSDT": "polkadot",
    "FILUSDT": "filecoin",
    "FETUSDT": "artificial-superintelligence-alliance",
    "INJUSDT": "injective-protocol",
    "LINKUSDT": "chainlink",
    "LTCUSDT": "litecoin",
    "MATICUSDT": "polygon",
    "NEARUSDT": "near",
    "OPUSDT": "optimism",
    "RENDERUSDT": "render-token",
    "SUIUSDT": "sui",
    "SEIUSDT": "sei-network",
    "STRKUSDT": "starknet",
    "SHIBUSDT": "shiba-inu",
    "TONUSDT": "the-open-network",
    "TRXUSDT": "tron",
    "UNIUSDT": "uniswap",
    "WLDUSDT": "worldcoin-wld",
    "XRPUSDT": "ripple",
    "DOGEUSDT": "dogecoin",
    "SOLUSDT": "solana",
    "BNBUSDT": "binancecoin",
    "PEPEUSDT": "pepe",
    "JTOUSDT": "jito-governance-token",
    "ACEUSDT": "fusionist",
    "BIGTIMEUSDT": "big-time",
    "BOMEUSDT": "book-of-meme",
    "WIFUSDT": "dogwifhat",
    "FLOKIUSDT": "floki",
    "BONKUSDT": "bonk",
    "JUPUSDT": "jupiter-exchange",
    "PYTHUSDT": "pyth-network",
    "IMXUSDT": "immutable-x",
    "GRTUSDT": "the-graph",
    "LDOUSDT": "lido-dao",
    "TIAUSDT": "celestia",
    "SAGAUSDT": "saga",
    "ENAUSDT": "ethena",
    "WUSDT": "wormhole"
}

# 提醒設定
ALERTS_FILE = "/tmp/weex_alerts.json"

# Railway 環境變數
RAILWAY_STATIC_URL = os.getenv("RAILWAY_STATIC_URL", "")
RAILWAY_PUBLIC_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")

# Keep-Alive 設定（防止 Railway 休眠）
KEEP_ALIVE_INTERVAL = int(os.getenv("KEEP_ALIVE_INTERVAL", "300"))
SELF_PING_URL = os.getenv("SELF_PING_URL", "")

# 執行狀態追蹤
START_TIME = datetime.now()
FETCH_STATS = {"total": 0, "success": 0, "fail": 0, "last_success": None}

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

async def self_ping(session: aiohttp.ClientSession):
    """自我喚醒, 防止 Railway 休眠"""
    url = SELF_PING_URL or f"http://localhost:{os.getenv('PORT', '8080')}/"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                logger.debug(f"Keep-alive ping OK")
    except Exception:
        pass

async def send_telegram(message: str, retry: int = 2):
    """發送 Telegram 通知（帶重試機制）"""
    if not bot or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram 未設定，跳過通知")
        return
    for attempt in range(retry + 1):
        try:
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=message,
                parse_mode="HTML",
                disable_web_page_preview=True,
                read_timeout=30, write_timeout=30, connect_timeout=30
            )
            logger.info(f"Telegram 已發送: {message[:50]}...")
            return
        except Exception as e:
            if attempt < retry:
                logger.warning(f"Telegram 發送失敗（重試 {attempt+1}/{retry}）: {e}")
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error(f"Telegram 發送失敗（已放棄）: {e}")

# ==================== 價格獲取（CoinGecko 為主 + Binance 備援）====================
async def fetch_from_coingecko(session: aiohttp.ClientSession) -> dict:
    """從 CoinGecko API 獲取價格"""
    ids = ",".join(COINGECKO_IDS.values())
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?ids={ids}"
        f"&vs_currencies=usd"
        f"&include_24hr_change=true"
        f"&include_24hr_vol=true"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status == 200:
                data = await resp.json()
                result = {}
                for symbol, coin_id in COINGECKO_IDS.items():
                    if coin_id in data:
                        d = data[coin_id]
                        result[symbol] = {
                            "price": float(d.get("usd", 0)),
                            "change24h": float(d.get("usd_24h_change", 0)),
                            "volume": 0,
                            "high": 0,
                            "low": 0,
                            "quoteVolume": float(d.get("usd_24h_vol", 0)),
                        }
                FETCH_STATS["success"] += 1
                FETCH_STATS["last_success"] = datetime.now().isoformat()
                logger.info(f"CoinGecko 成功獲取 {len(result)} 個幣種價格")
                return result
            elif resp.status == 429:
                logger.warning(f"CoinGecko rate limit (429)，嘗試 Binance 備援")
                return {}
            else:
                logger.error(f"CoinGecko API 回傳狀態碼: {resp.status}")
                return {}
    except asyncio.TimeoutError:
        logger.warning("CoinGecko 請求超時，切換至 Binance 備援")
        return {}
    except Exception as e:
        logger.error(f"CoinGecko 請求失敗: {e}")
        return {}

async def fetch_from_binance(session: aiohttp.ClientSession) -> dict:
    """從 Binance 合約 API 獲取價格（備援）"""
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
                logger.info(f"Binance 成功獲取 {len(result)} 個幣種價格")
                return result
            else:
                logger.error(f"Binance API 回傳狀態碼: {resp.status}")
                return {}
    except asyncio.TimeoutError:
        logger.warning("Binance 請求超時")
        return {}
    except Exception as e:
        logger.error(f"Binance 請求失敗: {e}")
        return {}

async def fetch_prices(session: aiohttp.ClientSession) -> dict:
    """獲取價格：CoinGecko 為主，失敗則用 Binance 備援"""
    # 先試 CoinGecko
    prices = await fetch_from_coingecko(session)
    if prices:
        return prices

    # CoinGecko 失敗，試 Binance
    logger.info("CoinGecko 未取得價格，切換至 Binance 備援")
    prices = await fetch_from_binance(session)
    if prices:
        return prices

    logger.error("CoinGecko 與 Binance 皆無法獲取價格")
    return {}

# ==================== 莊家指數計算 ====================
def calculate_whale_index(price: float, volume: float, change24h: float, quote_volume: float) -> dict:
    """計算莊家指數和信號（確定性版本）"""
    # 成交量分數 (0-35)
    volume_score = min(quote_volume / 50000000, 1) * 35

    # 漲跌動能分數 (0-35)
    momentum = abs(change24h)
    momentum_score = min(momentum * 2, 35)

    # 價格趨勢分數 (0-30)
    if change24h >= 5:
        trend_score = 30
    elif change24h >= 2:
        trend_score = 20
    elif change24h >= -2:
        trend_score = 15
    elif change24h >= -5:
        trend_score = 10
    else:
        trend_score = 5

    index = round(volume_score + momentum_score + trend_score)
    index = max(10, min(99, index))

    # 信號邏輯：基於漲跌方向
    if change24h >= 3:
        signal = "買入"
    elif change24h <= -3:
        signal = "賣出"
    else:
        signal = "持有"

    return {"index": index, "signal": signal}

# ==================== 提醒系統 ====================
class AlertManager:
    def __init__(self):
        self.alerts = self.load_alerts()
        self.triggered = set()
        self.price_history = {}

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
        """一鍵新增多空全套提醒（6種）"""
        long_entry = round(base_price * 0.985, 6)
        long_tp = round(base_price * 1.10, 6)
        long_sl = round(base_price * 0.92, 6)
        short_entry = round(base_price * 1.015, 6)
        short_tp = round(base_price * 0.90, 6)
        short_sl = round(base_price * 1.08, 6)

        added = []
        alert_configs = [
            ("entry", long_entry, "多單入場"),
            ("tp", long_tp, "多單止盈"),
            ("sl", long_sl, "多單止損"),
            ("short_entry", short_entry, "空單入場"),
            ("short_tp", short_tp, "空單止盈"),
            ("short_sl", short_sl, "空單止損"),
        ]

        for alert_type, price, label in alert_configs:
            exists = any(a["symbol"] == symbol and a["type"] == alert_type and not a.get("triggered") for a in self.alerts)
            if not exists:
                alert = self.add_alert(symbol, alert_type, price, note)
                added.append((label, price))

        return added

    def check_alerts(self, symbol: str, price: float, data: dict) -> list:
        """檢查並觸發提醒"""
        triggered = []
        whale = calculate_whale_index(price, data.get("volume", 0), data.get("change24h", 0), data.get("quoteVolume", 0))

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
                if whale["signal"] == "買入" and price <= target_price * 1.005:
                    should_trigger = True
            elif alert_type == "tp":
                if price >= target_price:
                    should_trigger = True
            elif alert_type == "sl":
                if price <= target_price:
                    should_trigger = True
                prev_index = prev.get("index")
                if prev_index and prev_index >= 40 and whale["index"] < 25:
                    should_trigger = True
                    alert["note"] = (alert.get("note", "") + " | 莊家出貨預警").strip(" |")
            elif alert_type == "short_entry":
                if whale["signal"] == "賣出" and price >= target_price * 0.995:
                    should_trigger = True
            elif alert_type == "short_tp":
                if price <= target_price:
                    should_trigger = True
            elif alert_type == "short_sl":
                if price >= target_price:
                    should_trigger = True
                prev_index = prev.get("index")
                if prev_index and prev_index <= 30 and whale["index"] > 70:
                    should_trigger = True
                    alert["note"] = (alert.get("note", "") + " | 莊家拉盤預警").strip(" |")

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
    logger.info(f"監控幣種: {len(COINS)} 個（合約）")
    logger.info(f"更新間隔: {UPDATE_INTERVAL} 秒")
    logger.info(f"Keep-Alive: 每 {KEEP_ALIVE_INTERVAL} 秒")
    logger.info(f"Telegram: {'已設定' if bot else '未設定'}")
    logger.info("=" * 50)

    if bot:
        startup_msg = (
            f"🚀 <b>WEEX 合約監控已啟動</b>\n"
            f"監控幣種: {len(COINS)} 個\n"
            f"更新間隔: {UPDATE_INTERVAL} 秒\n"
            f"運行環境: Railway"
        )
        await send_telegram(startup_msg)

    async with aiohttp.ClientSession() as session:
        loop_count = 0
        while True:
            try:
                start_time = time.time()
                FETCH_STATS["total"] += 1
                loop_count += 1

                # Keep-Alive: 定期自我喚醒
                if loop_count % max(1, KEEP_ALIVE_INTERVAL // UPDATE_INTERVAL) == 0:
                    await self_ping(session)

                prices = await fetch_prices(session)

                if not prices:
                    FETCH_STATS["fail"] += 1
                    logger.warning("本次未獲取到價格數據")
                    await asyncio.sleep(UPDATE_INTERVAL)
                    continue

                messages = []
                for symbol, data in prices.items():
                    try:
                        price = data["price"]
                        triggered = alert_manager.check_alerts(symbol, price, data)

                        for alert in triggered:
                            type_emoji = {
                                "entry": "🟢", "tp": "💰", "sl": "⚠️",
                                "short_entry": "🔴", "short_tp": "💰", "short_sl": "⚠️"
                            }
                            type_name = {
                                "entry": "入場", "tp": "止盈", "sl": "止損",
                                "short_entry": "空單入場", "short_tp": "空單止盈", "short_sl": "空單止損"
                            }
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
                    except Exception as coin_err:
                        logger.error(f"處理 {symbol} 時出錯: {coin_err}")
                        continue

                for msg in messages:
                    await send_telegram(msg)

                elapsed = time.time() - start_time
                sleep_time = max(1, UPDATE_INTERVAL - elapsed)
                uptime = datetime.now() - START_TIME
                logger.info(
                    f"輪詢完成，{len(prices)} 個幣種，"
                    f"耗時 {elapsed:.2f}s，已運行 {uptime.total_seconds()/3600:.1f}h，"
                    f"成功率 {FETCH_STATS['success']}/{FETCH_STATS['total']}，"
                    f"下次更新 {sleep_time:.0f}s 後"
                )
                await asyncio.sleep(sleep_time)

            except Exception as e:
                FETCH_STATS["fail"] += 1
                logger.error(f"主循環錯誤: {e}")
                await asyncio.sleep(UPDATE_INTERVAL)

# ==================== HTTP 伺服器 ====================
from aiohttp import web

async def health_check(request):
    return web.Response(text="OK", status=200)

async def get_status(request):
    uptime = datetime.now() - START_TIME
    return web.json_response({
        "status": "running",
        "coins_monitored": len(COINS),
        "alerts_total": len(alert_manager.alerts),
        "alerts_triggered": len(alert_manager.triggered),
        "update_interval": UPDATE_INTERVAL,
        "keep_alive_interval": KEEP_ALIVE_INTERVAL,
        "uptime_seconds": int(uptime.total_seconds()),
        "uptime_human": str(uptime).split(".")[0],
        "fetch_stats": FETCH_STATS,
        "telegram_configured": bool(bot and TELEGRAM_CHAT_ID),
        "railway_domain": RAILWAY_PUBLIC_DOMAIN or RAILWAY_STATIC_URL
    })

async def add_alert_api(request):
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
    runner = await start_web_server()
    await main()

if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop:
        loop.create_task(run())
    else:
        asyncio.run(run())
