# V5.11 Structure First 增補（2026-09-28）

正式版本仍由 scanner_contract.VERSION 定義，保留 V5.11。implementation_revision 為 sequence-open-space-20260928。基底為 cc113d9（已核對遠端 HEAD）。

## 排序與隔離

1H：4H hard filter（先排除）→ 1H 主結構品質 → 壓縮／再擴張 → 1D 條件式背景 → 4H 延續 → Key K 品質／階段 → 上方空間 → BTC RS → 資金確認 → auxiliary。採字典序，後層不能越過前層。
4H 維持獨立全市場評估；使用 1D 背景、4H 結構、壓縮／再擴張、Key K、空間、RS、資金的延續排序，不以 1H 候選作母池。

1D 分為 bullish_divergence、high_consolidation、bullish_reexpansion、base_turning_bullish、unconfirmed_base、bearish_divergence。一般優先序為前四種依序；未確認築底只在 reserve。完整空頭硬排除（本次明確新增對 1H 的 1D 完整空頭排除）。高位盤整且 4H 合格 Key K 之後再擴張，背景層提升為特殊優先，仍不能越過 1H 主結構層。

最近 12 根合格 Key K、1H 三型、S1–S5 判斷不改。bars_ago=0 僅標示「★ 本輪新關鍵K」，移除直接新舊排序加分。Key K 對當下結構的實際影響仍正常反映。各正式榜、備援榜、特殊榜最多 10；交集只有兩個正式榜共同 symbol，不重新評分。freshness 同時保護備援與交集。

## 特徵與數值解讀

- 最近 96 根已收盤資料：多頭排列持續根數、EMA15 同時領先 SMA30/45 根數。到達回看上限以 duration_censored 標示，並非宣稱更早沒有趨勢。
- Swing：左右各 2 根嚴格高／低點；最後 2 根尚未確認者不使用。HH/HL 僅影響品質，不新增 hard filter。
- ATR：最近 14 根 true range 的簡單平均。回檔由最近 24 根、排除最新 3 根的最高點起算，追蹤低點、收復、SMA30/45 收盤及影線失守。
- 重新擴張：回檔低點之後，兩組均線間距均擴大且仍為多頭排列。深度超过 3 ATR、價格超出 EMA15 3 ATR 後開始扣品質；這些是可檢視的策略數值化選擇，未宣稱經報酬回測最佳化。
- 1D 高位門檻：最近 24 根曾多頭、現價仍在 SMA45 以上，且距 24 根高點不超過 15%。再發散需先出現兩組均線間距收縮，再連續 3 根擴張。分類是規則模型，不是人工圖形判讀的唯一答案。
- 空間：96 根內已確認 swing high 及前高，取現價上方最近壓力，以 ATR 標準化；找不到壓力標示 no_resistance_in_96_bars，空間排序鍵為 [1, 0]；有壓力為 [0, remaining_atr]，不截斷 ATR。僅在原本空間層比較，不能超越前面的結構／背景／Key K。remaining_atr 在無阻力時為 null，不宣稱無限空間。

## 資金與缺資料處理

3／6 根 OI 各需 4／7 個精確時間邊界點；中途缺點不跨洞計算。價格報酬使用相同邊界的收盤價。提供逐根 CVD Proxy 與 OI 點，以及 3／6 根聚合。
CVD Proxy = sum(2 × taker_buy_base − volume)，必須有完整已收盤窗口。最近 3 根買賣量差占比小於前 3 根時標示轉弱並減少確認。
價格上漲且 OI 下降至少 1% 時標示 short-covering／deleveraging 疑慮；只是疑慮而非交易者意圖的證明。Funding 僅在 >0.001 時提示過熱及扣輔助分，不給方向性買進確認。

資金查詢僅針對各榜可進 Top10 的技術候選及同分邊界，因資金是最後兩層，其他技術候選不可能靠資金超車；技術掃描仍涵蓋全市場。未返回／缺少精確窗口／新上市歷史不足，保留 null 與 partial，不補造數值。

資料源：Binance USD-M klines、openInterestHist。CVD 仍是單一交易所的 Proxy，非跨交易所真實累積 CVD。OI 公開歷史窗口與API供應限制仍適用。
官方介面：[Binance Market Data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)

## 驗證與發布

執行專案目錄內 pytest；本次新增 test_structure_sequence.py。全市場驗證輸出由本次工作目錄 outputs 提供。
本次工作是本機修改／驗證，不自動部署線上服務。部署時須使用整包原始碼，清除或等待舊 completed snapshot 被新掃描替換，並核對 implementation_revision；不能僅憑 V5.11 名稱判定新邏輯已上线。

## sequence / open-space 輸出契約

新增 candidate-output.schema.json；summary 為 scanner-summary-v3，candidate.ranking_key_schema 為 lexicographic-open-space-v1。候選從計算層產出欄位，/scan/feed 與 /scan/feed/summary、MCP summary 原樣傳遞。舊 snapshot 不會被顯示層補造成新資料；部署後必須完成新一輪掃描並核對 implementation_revision。

- trend_duration_bars / ema15_lead_bars：原有 trend_duration / ema15_lead_duration 的穩定別名，舊欄保留。
- pullback_quality：結構化 depth_atr、recovered_atr、structure_broken，直接取原計算；不新增主觀 strong/weak 門檻。
- ma_defense：保留 SMA30/SMA45，增加 EMA15 的相同守位計算；HL 只在兩個已確認低點構成 higher low 時提供價格。從該低點右側兩根收盤確認起計算失守／收復。無已確認 HL 則各值 null。EMA15/HL 新欄不納入原 structure_quality 計分。
- swing_structure：hh/hl 為既有已確認 swing 比較；缺少兩個高／低點時為 null。broken 缺低點為 null；status 依序為 broken、兩者均成立的 intact、任一已知不成立的 weakening，剩餘資料不足為 null。
- reexpansion_quality：保留既有數值及計算；pullback.reexpansion 保留是否確認的布林值。
- extension_atr：原 pullback.overextension_atr；overextended 對應既有超過 3 ATR 開始扣分的門檻。
- upside_space：open_space=1 / resistance_class=open_space 代表96根內未偵測到主要阻力；measured 為 0。ranking_key 使用二元素子陣列；無阻力的第二個 0 僅為同類比較佔位，remaining_atr 與舊 score 皆 null。measured 使用未截斷 remaining_atr。消費端不得再使用 score 排名。
- 完整排名的層次與位置不变。JSON 陣列比較必須按層遞迴做數字字典序降冪；同為 open_space 時繼續比較 BTC RS 等後層。

掃盤文字輸出應逐檔讀取上述欄位，列出趨勢／EMA15 根數、回檔深度與收復、EMA15/SMA30/SMA45/HL 守位、HH/HL、再擴張品質與 ATR 延伸；null 顯示 N/A，不依文字補猜。
