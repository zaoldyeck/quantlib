# 本地 macOS launchd 排程(唯一真源)

本目錄 = 本機 launchd plist 的版控藍圖(2026-07-24 事故:plist 只活在 ~/Library/LaunchAgents,
rename 後指舊結構 `research.*` 每日 exit 1 無人察覺)。**雲端 VM 的 systemd units 真源在
infra repo(trading-bots-infra/gcp-quantlib/scripts/systemd/),不在這裡。**

## 套用

```bash
cp deploy/launchd/com.quantlib.intraday-pull.plist ~/Library/LaunchAgents/
launchctl unload ~/Library/LaunchAgents/com.quantlib.intraday-pull.plist 2>/dev/null
launchctl load ~/Library/LaunchAgents/com.quantlib.intraday-pull.plist
```

- `com.quantlib.intraday-pull`:每日 08:30 永豐 1 分 K 回補(quantlib.intraday.pull_kbars;
  log 在 var/log/intraday_pull.log)。
