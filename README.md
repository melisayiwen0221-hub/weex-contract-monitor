# WEEX 合約監控 Pro

24小時自動監控 Binance 合約價格，Telegram 即時推播入場/止盈/止損/莊家出貨預警。

## 部署步驟

### 1. 申請 Telegram Bot
- 找 @BotFather，輸入 `/newbot`
- 拿到 **Token** 和 **Chat ID**（@userinfobot）

### 2. 部署到 Railway
1. 登入 [Railway](https://railway.app)（用 GitHub 帳號）
2. New Project → Deploy from GitHub repo
3. 選這個 repo
4. 在 Variables 新增：
   - `TELEGRAM_TOKEN` = 你的 Bot Token
   - `TELEGRAM_CHAT_ID` = 你的 Chat ID
   - `UPDATE_INTERVAL` = `10`（秒，建議 10-30）
5. 點 Deploy

### 3. 防止休眠（UptimeRobot）
1. 登入 [UptimeRobot](https://uptimerobot.com)
2. Add New Monitor → HTTP(s)
3. URL 填 Railway 給你的網址（如 `https://xxx.up.railway.app`）
4. Monitoring Interval 設 5 分鐘
5. 儲存

### 4. 新增提醒
透過 API 或直接在 Telegram 跟 Bot 對話（未來擴充）。

## API 端點

| 端點 | 說明 |
|------|------|
| `GET /` | 健康檢查 |
| `GET /status` | 查看監控狀態 |
| `POST /alert` | 新增提醒（JSON: `{symbol, price, note}`）|

## 幣種清單

25 個幣種：BTC, ETH, ADA, AVAX, AAVE, ARB, APT, DOT, FIL, FET, INJ, LINK, LTC, MATIC, NEAR, OP, RENDER, SUI, SEI, STRK, SHIB, TON, TRX, UNI, WLD
