# EagleEye 公開資料與每日更新

EagleEye 是由瀏覽器讀取靜態快照、計算及繪圖的金融觀測網站。訪客不需要帳號、API key、資料庫或AI服務。頁面列出各指標自己的資料日期、來源及限制；報表建立時間不等於行情日期。

## 資料如何更新

- 能從瀏覽器取得的來源繼續由訪客瀏覽器直取，包括 Coin Metrics 日資料與既有行情來源；工作集留在記憶體。每日工作流程不永久保存這些工作集或逐筆成交。
- 官方來源中需要累積的最小歷史保存於 `cloud-data/`。目前每日收集範圍為 TWSE 已登錄市場CSV、OpenAPI小型股指數與排除台積電模型三檔、證期局月報、FRB H.4.1/H.10，以及台灣官方休市表。
- 先前已公開、仍符合現行公開政策的其他歷史可以明確匯入保留；未列入每日收集的來源維持原日期，不宣稱已自動更新。
- GitHub runner 只在單次工作期間暫用現有 DuckDB 程式產生相容快照，DB及原始下載檔不進repo、不成為外部資料庫服務。永久檔案為精簡觀測、修訂、必要模型欄位、來源狀態及來源雜湊。
- `site/` 只是可重建的網站成品；資料以 `cloud-data/` 為準。不得用較舊本機網站快照覆蓋此台帳。

來源失敗時保留原資料日期與失敗狀態，不以舊值冒充最新。通過檢查的其他來源可以保存。格式／完整性／發布檢查失敗時不部署；部署失敗可重新部署已驗證版本，不必再擷取。資料缺漏不補零，未知發布日保持未知。

## 排程與免費使用條件

此套件附帶尚待擁有者啟用的工作流程。預設 repo variable `EAGLEEYE_CLOUD_ENABLED` 未設或不等於字串 `true` 時，雲端取數與部署工作不執行。模板已存在不代表排程已啟用。

預定每天台北時間 **07:17** 與 **09:47** 檢查（UTC 23:17與01:47），包含週末，以涵蓋可能隔日才更新的最新日端點。第二次是有界檢查，是否取數依各來源的間隔及狀態決定。GitHub排程可能延遲或漏跑，公開倉60日無活動也可能停用排程；不是準點保證。

僅針對公開 `musiclord/EagleEye` 倉庫的標準 Ubuntu runner 設計，未使用 larger runner、外部資料庫或長效個人token。公開倉的標準runner目前免費；Pages與artifact仍受服務上限約束。Pages artifact只保留1日，不把artifact/cache當永久資料台帳。[GitHub Actions費用條件](https://docs.github.com/en/billing/concepts/product-billing/github-actions)、[排程限制](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)、[Pages限制](https://docs.github.com/en/pages/getting-started-with-github-pages/github-pages-limits)。

## 部署一致性

每日工作流程在同一次執行內收集、驗證、提交必要檔案與部署。它不依賴 `GITHUB_TOKEN` 推送再觸發另一個push工作流程。各入口共用 `pages` concurrency；遠端基底已前進或非快轉推送時停止，不強制覆寫遠端。

若已提交成功版本，但Pages部署暫時失敗，可手動執行每日流程並勾選 `redeploy_existing`；這只驗證並重新部署既有網站成品，不取數。此動作仍須擁有者啟用上述變數。

## 資料口徑

證期局月報與每日資料分開；表19自營商及外資分類重疊，不相加為互斥法人總額。排除台積電是申報普通股數的自建估算，並非官方加權指數；完整候選與基日固定有價樣本分列，缺價／未確認公司行動會中斷。必須有兩個獨立有效交易日才建立價格鏈。

Coin Metrics供給是代幣數，兩幣市值不代表全市場穩定幣或資金淨流入；BTC實現波動率不等於選擇權隱含波動率。來源出處、資料授權與改作說明列在網頁；資料授權與程式碼授權分開。

精簡欄位及公式可用於重算；原始資料hash本身不能還原原文。這份公開台帳不是完整raw封存，也不是擷取開始以前的真實當時可得歷史。
