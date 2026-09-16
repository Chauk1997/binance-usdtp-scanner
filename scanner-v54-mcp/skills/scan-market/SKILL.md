---
name: scan-market
description: 使用者要求「掃盤」、Binance USDT 永續合約全市場掃描時，呼叫 Railway V5.4 並呈現既有鎖定策略的結果。
---

呼叫本套件的 `scan_market_v54`，只使用這次回傳資料。API 固定為 Railway `/scan/feed`，沒有策略參數。一次呼叫可能需要約 41 秒；不並行掃描或自動重試。

確認 `status=complete`、`scanner=V5.4_CHATGPT_FEED`、`strategy=V5.2_LOCKED`，再以繁體中文呈現：

- 產生時間 UTC 與台北時間、版本、策略。產生時間不代表行情或 K 線已更新。
- 1H 與 4H 各自：entry 前10筆（不足10筆如實顯示）、wait_stop 等止跌K、wait_pullback 等回補、overextended 過度延伸勿追。
- 1H×4H：直接呈現 resonance，不自行取交集或重算分數。

沿用陣列順序、分類、原始數值。entry 可用表格顯示幣種、分數、結構分、daily_pct、vs_btc_pct、resistance_pct、止跌K與衍生品判定。watchlists 保留所有幣種，可用緊湊列表搭配分數。空陣列標示「本次無符合標的」；缺欄位或 null 標示「未提供」，不要視為零或無符合。

`stop` 代表止跌K訊號，不是停損價。API 未提供的進場價、停損價、槓桿或資金配置不要自行補造。只呈現 scanner 分類，不增加新的交易判斷。

status 非 complete、版本不符、資料格式不完整或請求失敗時，說明本次未取得完整結果；可呼叫 get_scanner_health 診斷。不要把歷史結果冒充本次結果，不呼叫 cache/init，不修改交易策略或排名。
