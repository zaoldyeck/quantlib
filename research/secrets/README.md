# 本機券商憑證(絕不進版控)

本目錄由 `research/.gitignore` 的 `secrets/*` 忽略(README 與 .gitkeep 除外);
根目錄 `.gitignore` 另有 `*.pfx / *.p12 / *.pem / *.key` 全域防護,
避免憑證被放到 repo 其他位置時失去保護(2026-07-21 實際發生過)。

## 現有憑證

| 券商 | 檔案 | 用途 | .env 設定 |
|---|---|---|---|
| 富邦 | `*.p12` | 下單(現役 S 策略雲端交易) | `FUBON_CERT_PATH` / `FUBON_CERT_PASSWORD` |
| 永豐 | `Sinopac.pfx` | **僅存放備用**;目前 Shioaji 只用於歷史行情下載,**資料查詢不需要憑證** | 未設(要下單時再加 `SHIOAJI_CERT_PATH` / `SHIOAJI_CERT_PASSWORD`) |

## 規則

1. 檔案權限一律 `600`(只有本人可讀):`chmod 600 <檔案>`
2. 憑證密碼只放 `research/.env`(同樣不進版控),**絕不寫進程式碼或對話**
3. 新增憑證放這裡,並在上表登記;不要放 repo 其他位置
4. 雲端(GCP VM)的憑證走 Secret Manager 注入,不從本機複製
