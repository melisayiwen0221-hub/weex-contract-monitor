# 🚀 WEEX 山寨幣拉盤監控系統

自動監控 WEEX 交易所 30 個熱門山寨幣，即時偵測莊家拉盤信號，並通過 Telegram 發送進場/止盈/止損建議。

## ✨ 功能特點

- **穩定運行**：自動重連、心跳保活、指數退避，確保 24/7 不間斷監控
- **即時偵測**：基於量比、價格漲幅、買賣盤失衡、大單買壓、動能分數的綜合拉盤偵測
- **智能建議**：自動計算進場價、止盈價、止損價
- **持倉監控**：進場後持續追蹤，觸發止盈止損或拉盤力度衰退時自動提醒出場
- **Telegram 通知**：即時推播，支持信號冷卻避免騷擾

## 📊 拉盤偵測指標

| 指標 | 說明 | 權重 |
|------|------|------|
| 量比 | 成交量相對平均值暴增倍數 | 25% |
| 價格漲幅 | 1分鐘/5分鐘/15分鐘漲幅 | 30% |
| 買賣盤失衡 | 買盤遠大於賣盤的程度 | 20% |
| 大單買壓 | 大額買單湧入比例 | 15% |
| 動能分數 | 綜合持續性評估 | 10% |

## 🚀 部署方式

### Railway 部署（推薦）

1. Fork 此專案到 GitHub
2. 在 Railway 創建新專案，連接 GitHub
3. 設置環境變數（見下方）
4. 部署完成！

### 本地運行

```bash
# 1. 克隆專案
git clone <repo-url>
cd weex-pump-tracker

# 2. 安裝依賴
pip install -r requirements.txt

# 3. 配置環境變數
cp .env.example .env
# 編輯 .env 填入你的 API Key 和 Telegram Token

# 4. 啟動
python main.py
```

### Docker 部署

```bash
docker build -t weex-tracker .
docker run -d --env-file .env --name weex-tracker weex-tracker
```

## ⚙️ 環境變數

| 變數 | 說明 | 必需 |
|------|------|------|
| `TELEGRAM_BOT_TOKEN` | Telegram Bot Token | ✅ |
| `TELEGRAM_CHAT_ID` | Telegram Chat ID | ✅ |
| `WEEX_API_KEY` | WEEX API Key | ❌ |
| `WEEX_API_SECRET` | WEEX API Secret | ❌ |
| `WEEX_PASSPHRASE` | WEEX API Passphrase | ❌ |
| `TRACK_SYMBOLS` | 自定義監控幣種（逗號分隔） | ❌ |

> 注意：WEEX API Key 用於獲取更精確的數據，但系統也可僅使用公開 WebSocket 運行。

## 📱 Telegram 設置

1. 找 [@BotFather](https://t.me/BotFather) 創建 Bot，獲取 Token
2. 發送 `/start` 給你的 Bot
3. 訪問 `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates` 獲取 Chat ID
4. 將 Token 和 Chat ID 填入環境變數

## 🛠️ 自定義配置

編輯 `src/pump_detector.py` 中的 `thresholds` 來調整偵測靈敏度：

```python
self.thresholds = {
    'volume_surge_min': 3.0,      # 量比至少 3 倍
    'price_change_1m_min': 0.005,  # 1分鐘漲幅 0.5%
    'price_change_5m_min': 0.015,  # 5分鐘漲幅 1.5%
    'ob_imbalance_min': 0.3,       # 買賣盤失衡 30%
    'whale_pressure_min': 0.4,     # 大單買壓 40%
    'momentum_min': 60,            # 動能分數 60
}
```

## 📁 專案結構

```
weex-pump-tracker/
├── main.py                  # 主程式入口
├── src/
│   ├── pump_detector.py     # 拉盤偵測引擎
│   ├── websocket_manager.py # 穩定 WebSocket 管理器
│   ├── weex_api.py          # WEEX API 適配器
│   └── telegram_notifier.py # Telegram 通知模組
├── requirements.txt         # Python 依賴
├── Procfile                 # Railway 配置
├── Dockerfile               # Docker 配置
└── .env.example             # 環境變數範例
```

## ⚠️ 免責聲明

此系統僅供學習和參考，不構成投資建議。加密貨幣交易風險極高，請自行評估風險並謹慎投資。

## 📄 License

MIT
