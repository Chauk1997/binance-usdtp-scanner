# 2026-09-27 最終施工規格變更

基準為已部署 9753fd0；以本文件覆蓋 STRATEGY.md / CODE_AUDIT.md 中舊有 5 秒啟動與 Direct CVD 敘述。既有 API、cache、retry、MCP、部署設定維持；舊策略路由保持停用。

- 正式版本 V5.11_CONFIRMED_20260927_PROXY_PLUS1M。
- 每小時 :01 啟動，包含台北 4H 00/04/08/12/16/20:01 與日線 08:01。手動／重啟補掃同樣等待收盤後 60 秒。expected 仍在真正收盤邊界前進，絕不倒退 expected 偽造 fresh。
- Binance server time + monotonic 作固定 cutoff。收盤前 cache 不因時鐘前進而升格；最新K需於收盤後60秒重新請求的證據。缺K短重試1/2/4秒，永久缺漏停止發布完整快照。
- pending 寬限改為150秒＝啟動等待60秒＋原發布預算90秒；pending 時 feed_ready=false 並隱藏受影響榜。超時仍缺最新快照才 stale。
- 每幣每週期 candle_audit 揭露 expected、available（實際取得的已收K）、scan（正式允許採用的K）、cache_final_confirmed；保留完整 /scan/feed，MCP 摘要不攜帶全母池診斷。
- CVD Proxy 正式取代舊 cvd 欄位與資格要求。公式 sum(2*taker_buy_base-volume)，與操作週期完整收盤窗口對齊，單位base asset quantity，窗口起點歸零。名稱固定 CVD Proxy；缺資料保留null，不要求Direct CVD，不用K棒顏色推算。
- 技術PASS後才查Auxiliary；Proxy參與最後順位排序、負值警示及與Taker<1同步退潮。
- 退潮沿用每榜至少5個弱標的、占整榜至少60%；可用 SCANNER_WARNING_MIN_SYMBOLS / SCANNER_WARNING_FRACTION 調整，非法值啟動失敗。全域需所有非空榜都達門檻；不足樣本明示。
- 五區維持1H Top10／4H Top10／Special Formal／Special Approaching／Market State。不足不補。候選增加technical_pass與主要理由；特殊型增加current_stage及timeframe。

## 型態來源稽核與保留範圍

檢查 9753fd0 strategy_latest.events、STRATEGY.md、CODE_AUDIT.md，及原對話與「貼上的文字 (1).txt」。原始對話有型態名稱與範例，但沒有完整人工演算法；既有文件已揭露工程解讀。本次沒有重新發明或更改 events、compression、trend、qualify 的篩選／排名公式。

保留精確程式定義：第一型＝既有四根壓縮＋向上KeyK＋收盤高於三線；第二型＝突破前24高點＋向上KeyK；第三型僅1H＝KeyK前後多頭、正報酬加速、EMA15–SMA45間距增大。共同向上條件仍為價格推進、EMA15與至少一條慢線向上；完整MA pull輸出三線數值。保留既有操作級別空頭與SMA45下方3%結構失效條件。型態名稱不加入排名鍵。

這符合「保留現有程式精確定義」，但不能據此宣稱已證實與未提供的歷史人工判圖100%一致。

回退：使用交付的 scanner-before.bundle（完整Git備份），或回退到9753fd0。新版本拒絕載入舊策略snapshot，回退後同樣會重新建快照。
