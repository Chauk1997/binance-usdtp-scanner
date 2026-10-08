# Oracle Always Free 部署候選（尚未部署）

只在使用者確認帳戶／home region、Always Free可用額度和主機存取後使用。
建議 A1 2 OCPU / 12GB、Ubuntu、50GB Always Free boot disk；同帳戶總額度仍需核對。
不建立付費資源，不啟用付費 load balancer／NAT，不使用 trial credit 作永久免費證據。

安裝Python3.12與venv，將本專案放 /opt/scanner，建立 scanner 系統帳戶，安裝 requirements.txt。
先以 scanner 使用者手動執行 run_phase16_once.py，SCANNER_DATA_DIR=/var/lib/scanner。
核對 /fapi/v1/time、exchangeInfo、klines、輔助endpoint可達，HTTP451視為地區不適用，不用代理繞過。
匯出JSON與SQLite備份到主機外（可用額度內免費物件儲存）；SQLite用 backup API，不直接複製運作中的WAL資料庫。

驗證成功後安裝 scanner.service 並啟動。單一worker使用內建每小時:01排程，4H只有新收盤邊界更新。
服務監聽127.0.0.1；先用SSH轉送測試API。公開HTTPS與MCP存取需另確認域名／TLS與存取方式。
MCP網址透過 SCANNER_BASE_URL 設定；保留既有Railway網址為legacy預設，遷移時明確改設。

驗收：完整24小時保存24個1H邊界、6個4H邊界（不把retry算新輪），重啟後signal_id/key_time不變，4H非收盤時間ranking不變，超24h不展示。
記錄每輪耗時／缺失／HTTP狀態／費用額度；Oracle容量及閒置回收仍是營運風險。沒有完成這些驗收不能聲稱部署成功。
