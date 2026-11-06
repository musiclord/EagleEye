# 鷹眼 EagleEye · 已發布觀測快照

**開啟儀表板：[musiclord.github.io/EagleEye](https://musiclord.github.io/EagleEye/)**

版本 0.8.0；本次快照整理於 2026-11-06（台北時間）。各指標保留自己的觀測期，發布日期不表示所有數據都更新到當天。

這個目錄只包含可發布的網頁與已整理官方資料；沒有 Python 服務、本機資料庫、原始下載檔、私人事件備註或通知匣。

在獨立 GitHub 儲存庫的 Settings → Pages → Build and deployment 選擇 **GitHub Actions**，再推送 main/master，或手動執行 Publish EagleEye snapshot。專案型 Pages 子路徑已使用相對資源網址。無須改動其他網站。

網頁可閱讀每日三主題、圖表、相關分析與情境試算；每項數據保留來源、觀測期及品質。它不是即時服務，瀏覽器不會自行更新來源資料、補抓或在關頁後發出通知。更新方式是在本機重新匯出 `site/`，審閱後推送。發布時間不等於資料觀測日。

原專案執行 `build-web.ps1` 及 `prepare-pages.ps1` 產生本目錄。GitHub Actions 工作流只部署已匯出的資料，不會下載金融數據。每日雲端更新需另設取得資料的工作流、可持續狀態與失敗監控。

資料屬各官方提供者；保留來源標示，不因程式可公開就宣稱來源資料可不受限制再散布。發布前依用途確認各來源使用條款。程式計算不是投資建議。
