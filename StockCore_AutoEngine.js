/**DEMO-2026.03.30
 *Refactor: 優化定期定額&股票分割 父單ID 回溯邏輯，導入時間軸配對 (Time-series mapping) 以解決多代設定單衝突
 * 股票管理系統 - 獨立腳本修復版 (Standalone Fix)
 * 修正重點：
 * 1. [強制指定ID] 移除所有 getActiveSpreadsheet()，改用 getSS()。
 * 2. [修正 Null 錯誤] 解決獨立腳本無法抓取當前試算表的問題。
 */

// ★★★ 1. 全域設定 ★★★
// 請確認這個 ID 是正確的 (從您的截圖看是對的)
const SHEET_ID = "1NiasjvSXYRBqWSVOVd367HyLbWsVMfuyQcxph4oEHzM"; 

// ★★★ 2. 共用小幫手 (核心修正) ★★★
function getSS() {
  // 強制使用 ID 開啟
  return SpreadsheetApp.openById(SHEET_ID);
}

// 寫入偵錯紀錄的輔助函式
function logToDebug(msg) {
  // ★ 修正：原本這裡用 getActiveSpreadsheet() 導致報錯，現在改用 getSS()
  const ss = getSS(); 
  let sheet = ss.getSheetByName("DebugLog");
  if (!sheet) {
    sheet = ss.insertSheet("DebugLog");
    sheet.appendRow(["時間", "訊息"]);
  }
  const time = Utilities.formatDate(new Date(), "GMT+8", "HH:mm:ss");
  sheet.appendRow([time, msg]);
}

function main() {
  var lock = LockService.getScriptLock();
  
  try {
    // 這裡改用 try-catch 包裹 logToDebug
    try { logToDebug("--- Bot 啟動 ---"); } catch(e) {}
    
    lock.waitLock(30000); 
    logToDebug("取得鎖定，開始執行");
  } catch (e) {
    logToDebug("排隊超時 (系統忙碌)");
    return;
  }

try {
    // 依序執行所有自動化任務
    // 1. 檢查並產生定期定額待辦
    checkAndExecuteDCA(); 
    
    // 2. 檢查並寫入股票分割里程碑
    checkAndExecuteSplits();
    
    // 3. 🕷️ 執行 Yahoo 除權息爬蟲 (★ 使用手動後台加入除權息資料，此處不執行。)
    // autoFetchDividend();
    
    // 4. 產出最新庫存報表
    const result = generateInventoryReport(); 
    
    logToDebug("執行結果: " + result);
  } catch (e) {
    logToDebug("🔥 發生嚴重錯誤: " + e.toString());
    console.error(e);
  } finally {
    lock.releaseLock();
    logToDebug("--- Bot 結束 ---");
  }
}

// ★★★ 核心運算：庫存報表產生器 (含股票分割修正) ★★★
function generateInventoryReport() {
  const ss = getSS();
  const txnSheet = ss.getSheetByName("Transactions");
  
  // --- 1. 智慧等待資料同步 ---
  let maxRetries = 10;
  let isDataReady = false;
  
  if (!txnSheet) {
    logToDebug("❌ 找不到 Transactions 分頁！");
    return "ERROR_NO_SHEET";
  }

  let lastRow = txnSheet.getLastRow();
  
  // 若是空表則視為準備好
  if (lastRow < 2) isDataReady = true;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    if (isDataReady) break;
    
    lastRow = txnSheet.getLastRow();
    // 檢查最後一行的 L 欄 (帳號) 是否已寫入
    let lastAccount = txnSheet.getRange(lastRow, 12).getValue(); 
    
    if (lastAccount && String(lastAccount).trim() !== "") {
      isDataReady = true;
      break; 
    } else {
      Utilities.sleep(1000);
    }
  }

  if (!isDataReady) {
    logToDebug("⚠️ 等待超時：L欄仍為空，強制繼續執行 (標記為 Unknown)");
  }

  // --- 2. 準備工作表 ---
  checkAndCreateSheet(ss, "Transactions");
  checkAndCreateSheet(ss, "DividendData"); 
  
  let reportSheet = ss.getSheetByName("InventoryReport");
  if (!reportSheet) {
    reportSheet = ss.insertSheet("InventoryReport");
  }

  const divSheet = ss.getSheetByName("DividendData");
  const now = new Date();
  // 設定為今日最後一刻，確保包含今日所有交易
  now.setHours(23, 59, 59, 999);

  // --- 3. 讀取與整理資料 ---
  if (txnSheet.getLastRow() < 1) return "EMPTY_TXN";
  
  const txns = txnSheet.getRange(1, 1, txnSheet.getLastRow(), 12).getDisplayValues();
  const divs = divSheet.getDataRange().getDisplayValues(); 
  
  let timeline = [];

// (A) 交易紀錄 (Transactions)
  for (let i = 1; i < txns.length; i++) {
    let row = txns[i];
    let date = new Date(row[1]); 
    let stockId = String(row[2]).trim(); 
    let memo = String(row[9] || "");        // ★ 抓取 J 欄 (備註)
    let accountRaw = String(row[11]).trim(); 

    // ★ 終極防護罩：如果備註包含「【異常】」，直接踢出時間軸，不計入庫存！
    if (memo.indexOf("【異常】") !== -1) {
      logToDebug(`🛡️ 隔離異常單據：跳過 ${stockId} 於 ${row[1]} 的交易`);
      continue; 
    }

    if (!accountRaw) accountRaw = "Unknown";
    
    // 補零邏輯
    if (/^\d+$/.test(stockId)) {
        while (stockId.length < 4) { stockId = "0" + stockId; }
        if (stockId.length === 4 && stockId.startsWith("0") && !stockId.startsWith("00")) { stockId = "0" + stockId; }
    }
    
    if (!stockId) continue; 
    if (isNaN(date.getTime()) || date > now) continue;

    timeline.push({
      type: 'TXN',
      date: date,
      stockId: stockId,
      action: row[4], 
      unit: row[5],
      price: parseFloat(row[6].replace(/,/g, '')) || 0,
      qty: parseFloat(row[7].replace(/,/g, '')) || 0,
            totalAmount: parseFloat(String(row[10]).replace(/,/g, '')) || 0, // ★ 新增這行抓 K欄(交割金額)
      accounts: accountRaw.split(",").map(s => s.trim())
    });
  }

  // (B) 除權息 (DividendData)
  for (let i = 1; i < divs.length; i++) {
    let row = divs[i];
    let stockId = String(row[1]).trim();
    let date = new Date(row[2]);
    if (!stockId || isNaN(date.getTime())) continue;
    if (date > now) continue;
    
    timeline.push({
      type: 'DIV', 
      date: date, 
      stockId: stockId, 
      cash: parseFloat(row[3]) || 0, 
      stock: parseFloat(row[4]) || 0
    });
  }

// (C) ★★★ 股票分割 (Splits) 動態讀取版 ★★★
  const splitSheet = ss.getSheetByName("SplitSettings");
  if (splitSheet) {
    const splitLastRow = splitSheet.getLastRow();
    if (splitLastRow > 1) {
      const splitData = splitSheet.getRange(2, 1, splitLastRow - 1, 3).getValues();
      splitData.forEach(row => {
        let sId = String(row[0]).trim();
        let sDate = new Date(row[1]);
        let sRatio = parseFloat(row[2]);
        
        // 確保資料有效，且時間已經到了才執行分割
        if (sId && !isNaN(sDate.getTime()) && sRatio > 0 && sDate <= now) {
          timeline.push({
            type: 'SPLIT',
            date: sDate,
            stockId: sId,
            ratio: sRatio
          });
        }
      });
    }
  }

  // (D) 時間軸排序
  timeline.sort((a, b) => {
    // 先按日期
    if (a.date.getTime() !== b.date.getTime()) return a.date.getTime() - b.date.getTime();
    // 同一天內：先處理分割(SPLIT) -> 再處理除權息(DIV) -> 最後處理交易(TXN)
    // 邏輯：當天開盤前通常已完成除權息/分割，交易發生在盤中
    const typeOrder = { 'SPLIT': 0, 'DIV': 1, 'TXN': 2 };
    return typeOrder[a.type] - typeOrder[b.type];
  });

  // --- 4. 核心計算 ---
  const summary = {}; 

  timeline.forEach(event => {
    // 4-1. 處理交易
    if (event.type === 'TXN') {
      let actualQty = (event.unit === "整張") ? event.qty * 1000 : event.qty;
      let fee = Math.max(20, Math.floor(event.price * actualQty * 0.001425));
      
      event.accounts.forEach(user => {
        if (!user) return;
        initSummary(summary, user, event.stockId);
        
        let s = summary[user][event.stockId];
        let netAmount = 0; 
        // ★ 修改：將「定期定額」與「買進」一視同仁
        if (event.action === "買進" || event.action === "定期定額") {
          
          // 若 K 欄有數字，就信任 K 欄(包含了您自訂的 1元手續費)。若為 0 則用舊算法防呆
          let defaultFee = Math.max(20, Math.floor(event.price * actualQty * 0.001425));
          let defaultCost = (event.price * actualQty) + defaultFee;
          
          netAmount = event.totalAmount > 0 ? event.totalAmount : defaultCost;

          s.qty += actualQty;
          s.rawCost += netAmount;
          s.adjCost += netAmount;       // 含息成本
          s.netCashCost += netAmount;   // 淨現金成本
          s.netCashCostNoDiv += netAmount; 
        } 
        else {
          // 賣出
          let tax = Math.floor(event.price * actualQty * 0.003); 
          let moneyBack = (event.price * actualQty) - tax - fee; 
          
          if (s.qty > 0) {
            // 依比例扣除成本
            let ratio = actualQty / s.qty;
            s.rawCost -= (s.rawCost * ratio);
            s.adjCost -= (s.adjCost * ratio);
            s.netCashCost -= moneyBack;      // 賣出拿回的錢，扣減投入成本
            s.netCashCostNoDiv -= moneyBack; 
            s.qty -= actualQty;
          }
        }
      });

    // 4-2. 處理除權息
    } else if (event.type === 'DIV') {
      for (let user in summary) {
        if (summary[user][event.stockId]) {
          let s = summary[user][event.stockId];
          if (s.qty > 0) {
            // 現金股利：降低含息成本 & 淨現金成本
            let totalCash = s.qty * event.cash;
            s.adjCost -= totalCash;
            s.netCashCost -= totalCash;
            
            // 股票股利：增加股數 (每股配 X 元股票 = 配 X/10 股)
            if (event.stock > 0) {
              s.qty += (s.qty * (event.stock / 10));
            }
          }
        }
      }

    // 4-3. ★★★ 處理股票分割 ★★★
    } else if (event.type === 'SPLIT') {
      for (let user in summary) {
        if (summary[user][event.stockId]) {
          let s = summary[user][event.stockId];
          if (s.qty > 0) {
            // 分割：股數變大，總成本不變
            // 例如 1拆4：股數 * 4
            logToDebug(`執行分割: ${user} 的 ${event.stockId} 股數 x ${event.ratio}`);
            s.qty *= event.ratio;
            // 成本不用動，平均成本自然會因為 qty 變大而降低
          }
        }
      }
    }
  });

  // --- 5. 輸出結果 ---
  const finalOutput = [
    ["ID", "股票代碼", "股票名稱", "目前庫存", "平均成本(不含息)", "平均成本(含息)", "平均成本(淨現金)", "平均成本(淨現金-不含息)", 
     "不賠本賣價(不含息)", "當下價格", "未實現損益(不含息)", "帳號"]
  ];
  const formulas = []; 
  formulas.push(["", "", ""]); 

  let rowIdx = 2; 
  for (let user in summary) {
    for (let id in summary[user]) {
      let s = summary[user][id];
      // 過濾掉已出清的股票 (極小誤差視為0)
      if (s.qty > 0.001) { 
        let avgRaw = s.rawCost / s.qty; 
        let avgAdj = s.adjCost / s.qty; 
        let avgNetCash = s.netCashCost / s.qty; 
        let avgNetCashNoDiv = s.netCashCostNoDiv / s.qty; 
        let breakEvenRaw = avgRaw / 0.995575; 
        
        let uniqueId = user + "_" + id;
        let yahooUrl = `"https://tw.stock.yahoo.com/quote/"&B${rowIdx}`;
        let nameFormula = `IFERROR(REGEXEXTRACT(IMPORTXML(${yahooUrl}, "//title"), "^(.+?)\\("), IFERROR(GOOGLEFINANCE("TPE:"&B${rowIdx}, "name"), IFERROR(GOOGLEFINANCE("TWO:"&B${rowIdx}, "name"), "")))`;

        finalOutput.push([
          uniqueId, 
          id,
          "", 
          Math.round(s.qty), // 股數四捨五入顯示
          avgRaw.toFixed(2), 
          avgAdj.toFixed(2), 
          avgNetCash.toFixed(2), 
          avgNetCashNoDiv.toFixed(2), 
          Math.ceil(breakEvenRaw * 100) / 100, 
          "", 
          "", 
          user 
        ]);

        let priceFormula = `IFERROR(GOOGLEFINANCE("TPE:"&B${rowIdx}), IFERROR(GOOGLEFINANCE("TWO:"&B${rowIdx}), IFERROR(IMPORTXML(${yahooUrl}&".TWO", "//span[contains(@class,'Fz(32px)')]"), "查無")))`;
        let profitFormula = `IF(ISNUMBER(J${rowIdx}), (J${rowIdx}-E${rowIdx})*D${rowIdx}, 0)`;
        
        formulas.push([`=${nameFormula}`, `=${priceFormula}`, `=${profitFormula}`]);
        rowIdx++;
      }
    }
  }

  // --- 6. 寫入試算表 ---
  if (finalOutput.length <= 1) { 
      logToDebug("❌ 結果為空 (只有標題)，不進行寫入");
      return "SKIP_EMPTY_RESULT"; 
  }

  // 確保是寫入 InventoryReport
  reportSheet = ss.getSheetByName("InventoryReport");
  
  // 先設定文字格式避免 0050 變 50
  reportSheet.getRange("A:B").setNumberFormat("@");
  // 寫入數據
  reportSheet.getRange(1, 1, finalOutput.length, 12).setValues(finalOutput);
  
  // 寫入公式
  if (formulas.length > 1) {
      let dataFormulas = formulas.slice(1); 
      reportSheet.getRange(2, 3, dataFormulas.length, 1).setFormulas(dataFormulas.map(r => [r[0]]));
      reportSheet.getRange(2, 10, dataFormulas.length, 2).setFormulas(dataFormulas.map(r => [r[1], r[2]]));
  }

  // 清除多餘舊資料
  const lastRowAfter = reportSheet.getLastRow();
  if (lastRowAfter > finalOutput.length) {
      reportSheet.getRange(finalOutput.length + 1, 1, lastRowAfter - finalOutput.length, 12).clearContent();
  }

  // 美化格式
  reportSheet.getRange("A1:L1").setBackground("#4285F4").setFontColor("white").setFontWeight("bold");
  let profitRange = reportSheet.getRange(2, 11, finalOutput.length - 1, 1);
  let rules = reportSheet.getConditionalFormatRules();
  reportSheet.clearConditionalFormatRules(); 
  let rule1 = SpreadsheetApp.newConditionalFormatRule().whenNumberGreaterThan(0).setFontColor("#CC0000").setRanges([profitRange]).build();
  let rule2 = SpreadsheetApp.newConditionalFormatRule().whenNumberLessThan(0).setFontColor("#009900").setRanges([profitRange]).build();
  rules.push(rule1, rule2);
  reportSheet.setConditionalFormatRules(rules);
  
  logToDebug("✅ 庫存報表更新成功 (含分割運算)");
  return "SUCCESS";
}

function initSummary(summary, user, stockId) {
  if (!summary[user]) summary[user] = {};
  if (!summary[user][stockId]) summary[user][stockId] = { qty: 0, rawCost: 0, adjCost: 0, netCashCost: 0, netCashCostNoDiv: 0 };
}
function checkAndCreateSheet(ss, name) { if (!ss.getSheetByName(name)) ss.insertSheet(name); }

// 以下函式也一併修正為 getSS()
function updateTransactionData() {
  const ss = getSS(); 
  const sheet = ss.getSheetByName("Transactions");
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return;
  sheet.getRange("C:C").setNumberFormat("@"); 
  sheet.getRange("G:G").setNumberFormat("0.00"); 
  sheet.getRange("K:K").setNumberFormat("$#,##0"); 
  const range = sheet.getRange(2, 1, lastRow - 1, 12);
  const values = range.getValues();
  const formulas = range.getFormulas(); 
  let isUpdated = false;
  for (let i = 0; i < values.length; i++) {
    let row = values[i];
    let rowIndex = i + 2; 
    let stockId = String(row[2]).trim(); 
    if (/^\d+$/.test(stockId)) { 
      while (stockId.length < 4) { stockId = "0" + stockId; }
      if (stockId.length === 4 && stockId.startsWith("0") && !stockId.startsWith("00")) { stockId = "0" + stockId; }
    }
    if (values[i][2] !== stockId) { values[i][2] = stockId; isUpdated = true; }
    let currentName = row[3];
    let currentFormula = formulas[i][3];

    let id = row[0];
    let price = parseFloat(row[6]) || 0;    
    let inputQty = parseFloat(row[7]) || 0; 
    if (id === "" && (price > 0 || inputQty > 0)) { values[i][0] = Utilities.getUuid(); isUpdated = true; }
    let action = row[4]; 
    let unit = row[5];   
    let currentTotal = row[10]; 
    let qtyRange = sheet.getRange(rowIndex, 8); 
    if (unit === "整張") { qtyRange.setNumberFormat('0"張"'); } else { qtyRange.setNumberFormat('0"股"'); }
    if (price > 0 && inputQty > 0) {
      let actualQty = (unit === "整張") ? inputQty * 1000 : inputQty;
      let fee = Math.floor(price * actualQty * 0.001425);
      if (fee < 20) fee = 20; 
      let tax = (action === "賣出") ? Math.floor(price * actualQty * 0.003) : 0;
      let finalAmount = (action === "買進") ? (Math.floor(price * actualQty) + fee) : (Math.floor(price * actualQty) - fee - tax);
      if (currentTotal !== finalAmount && currentTotal === "") { values[i][10] = finalAmount; isUpdated = true; }
    }
  }
  if (isUpdated) { range.setValues(values); }
}

function updateMasterStockList() {
  const ss = getSS();
  let listSheet = ss.getSheetByName("StockList");
  if (!listSheet) { listSheet = ss.insertSheet("StockList"); listSheet.appendRow(["股票代號", "股票名稱", "搜尋標籤"]); listSheet.getRange("A1:C1").setBackground("#ea4335").setFontColor("white").setFontWeight("bold"); }
  let stockMap = new Map();
  try {
    const urls = ["https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"];
    urls.forEach(url => {
      const response = UrlFetchApp.fetch(url);
      const html = response.getBlob().getDataAsString("MS950");
      const rows = html.split("<tr>");
      rows.forEach(row => {
        const cols = row.split("</td>");
        if (cols.length > 3) {
          let codeName = cols[0].replace(/<[^>]+>/g, "").trim(); 
          let type = cols[2].replace(/<[^>]+>/g, "").trim(); 
          let parts = codeName.split(/　| /); 
          if (parts.length >= 2) {
            let code = parts[0].trim();
            let name = parts[1].trim();
            if (/^[0-9A-Z]{4,6}$/.test(code)) {
               let isStandard = (code.length === 4);
               let isETF = (type.indexOf("ETF") !== -1 || type.indexOf("證券") !== -1 || type.indexOf("債") !== -1);
               let isBond = code.endsWith("B");
               if (isStandard || isETF || isBond) { stockMap.set(code, name); }
            }
          }
        }
      });
    });
  } catch (e) { console.log("爬蟲失敗: " + e.toString()); }
  try {
    const txnSheet = ss.getSheetByName("Transactions");
    if (txnSheet && txnSheet.getLastRow() > 1) {
      const txnData = txnSheet.getRange(2, 3, txnSheet.getLastRow() - 1, 2).getValues();
      txnData.forEach(row => {
        let code = String(row[0]).trim();
        let name = String(row[1]).trim();
        if (code && name && name !== "#N/A" && name !== "Loading..." && name !== "查無名稱") {
           if (!stockMap.has(code)) { stockMap.set(code, name); }
        }
      });
    }
  } catch (e) { console.log("Transactions讀取失敗: " + e.toString()); }
  if (stockMap.size > 0) {
    let allStocks = [];
    stockMap.forEach((name, code) => { allStocks.push([code, name, `${code} ${name}`]); });
    allStocks.sort((a, b) => a[0].localeCompare(b[0]));
    const lastRow = listSheet.getLastRow();
    if (lastRow > 1) { listSheet.getRange(2, 1, lastRow - 1, 3).clearContent(); }
    listSheet.getRange(2, 1, allStocks.length, 1).setNumberFormat("@");
    listSheet.getRange(2, 1, allStocks.length, 3).setValues(allStocks);
  }
}
// ★★★ 功能：定期定額自動生成器 (精準 12 欄位版) ★★★
// ★★★ 功能：定期定額自動生成器 (支援預計停扣日 & 股利再投入) ★★★
// ★★★ 功能：定期定額自動生成器 (修復歷史停扣單回溯問題) ★★★
function checkAndExecuteDCA() {
  const ss = getSS();
  const dcaSheet = ss.getSheetByName("DCASettings");
  const txnSheet = ss.getSheetByName("Transactions");
  
  if (!dcaSheet || !txnSheet) return;
  logToDebug("⏰ 開始檢查定期定額設定...");

  // 1. 建立已存在的 DCA 紀錄 Set
  let existingDCA = new Set();
  const txnLastRow = txnSheet.getLastRow();
  if (txnLastRow > 1) {
    const txnData = txnSheet.getRange(2, 1, txnLastRow - 1, 14).getValues(); 
    txnData.forEach(row => {
      if (row[4] === "定期定額") { // E欄
        let dateStr = Utilities.formatDate(new Date(row[1]), "GMT+8", "yyyy-MM-dd");
        existingDCA.add(`${row[2]}_${dateStr}_${row[8]}_${row[11]}`); // 代碼_日期_券商_帳號
      }
    });
  }

  const today = new Date();
  today.setHours(23, 59, 59, 999);
  
  const dcaLastRow = dcaSheet.getLastRow();
  if (dcaLastRow < 2) return;
  
  const settings = dcaSheet.getRange(2, 1, dcaLastRow - 1, 11).getValues(); 
  let newTxns = [];

  // 2. 逐一檢查設定檔
  settings.forEach((row, index) => {
    let dcaSettingId = row[0]; // ★ 抓取 A欄：定期定額設定單的 ID
    let stockId = String(row[1]).trim();
    let checkDay = parseInt(row[2]);
    let amount = row[3];
    let feeSetting = String(row[4]).trim(); 
    let startDateRaw = row[5];
    let broker = row[6];                    
    let account = row[7];                   
    let isEnabled = row[8];                 
    let stopDateRaw = row[9];               // 預計停扣日
    let autoReinvest = row[10];             // 股利再投入
    
    if (!startDateRaw || !stockId) return;

    let stopDate = null;
    if (stopDateRaw) {
      stopDate = new Date(stopDateRaw);
      stopDate.setHours(23, 59, 59, 999);
      
      // 自動停扣邏輯：今天大於停扣日，且原本還在啟用中，則自動關閉
      if (today > stopDate && isEnabled === true) {
        dcaSheet.getRange(index + 2, 9).setValue(false); 
        isEnabled = false;
        logToDebug(`⏸️ ${stockId} 已過預計停扣日，自動變更為停扣。`);
      }
    }

    // ★ 關鍵修正：計算這筆設定的「最終有效日期」
    let effectiveEndDate = today; // 預設算到今天
    
    if (stopDate) {
      // 如果有填停扣日，就只算到停扣日 (或今天，取較早者)
      effectiveEndDate = stopDate < today ? stopDate : today;
    } else {
      // 如果沒填停扣日，且又被手動停扣(N)，因為不知道哪天停的，只好跳過不補單
      if (isEnabled !== true) return; 
    }

    let startDate = new Date(startDateRaw);
    let iterDate = new Date(startDate.getFullYear(), startDate.getMonth(), 1);
    
    // ★ 迴圈只會跑到「最終有效日期」為止
    while (iterDate <= effectiveEndDate) {
      let dcaDate = new Date(iterDate.getFullYear(), iterDate.getMonth(), checkDay);
      
      if (dcaDate.getMonth() !== iterDate.getMonth()) {
        dcaDate = new Date(iterDate.getFullYear(), iterDate.getMonth() + 1, 0);
      }

      // 檢查扣款日是否在「開始日」與「最終有效日期」之間
      if (dcaDate >= startDate && dcaDate <= effectiveEndDate) {
        let dateStr = Utilities.formatDate(dcaDate, "GMT+8", "yyyy-MM-dd");
        let uniqueKey = `${stockId}_${dateStr}_${broker}_${account}`;
        
        if (!existingDCA.has(uniqueKey)) {
          // 寫入 15 個欄位 (新增 O 欄: 父單ID)
          newTxns.push([
            Utilities.getUuid(),           // A: ID
            dateStr,                       // B: 交易日期
            stockId,                       // C: 股票代碼
            "",                            // D: 股票名稱
            "定期定額",                    // E: 操作項目
            "零股",                        // F: 單位
            0,                             // G: 價格
            0,                             // H: 數量
            broker,                        // I: 證券商
            "【待補價量】",                 // J: 備註
            0,                             // K: 交割金額
            account,                       // L: 帳號
            false,                         // M: 本期含股利再投入 (★強制預設不勾選)
            0,                             // N: 投入股利金額 (★預設為0)
            dcaSettingId                  // O: 父單ID (★成功綁定定期定額設定單)
          ]);
          existingDCA.add(uniqueKey); 
        }
      }
      iterDate.setMonth(iterDate.getMonth() + 1);
    }
  });

  // 3. 寫入試算表(範圍改為 15 欄)
if (newTxns.length > 0) {
    const txnLastRow = txnSheet.getLastRow();
    txnSheet.getRange(txnLastRow + 1, 1, newTxns.length, 15).setValues(newTxns);
    logToDebug(`✅ 成功追溯並產生 ${newTxns.length} 筆定期定額待辦紀錄！(已綁定父單ID)`);
  } else {
    logToDebug("定期定額檢查完畢，目前無須補登紀錄。");
  }
}

// ★★★ 功能：股票分割自動紀錄器 (自動綁定交易帳號版) ★★★
// ★★★ 功能：股票分割自動紀錄器 (自動綁定帳號 & 股票名稱版) ★★★
// ★★★ 功能：股票分割自動紀錄器 (精準判斷分割日前庫存版) ★★★
function checkAndExecuteSplits() {
  const ss = getSS();
  const splitSheet = ss.getSheetByName("SplitSettings");
  const txnSheet = ss.getSheetByName("Transactions");
  const stockListSheet = ss.getSheetByName("StockList");

  if (!splitSheet || !txnSheet) return;

  const now = new Date();
  const splitLastRow = splitSheet.getLastRow();
  if (splitLastRow < 2) return;

  // ★ 1. 建立股票名稱字典
  let stockNameMap = {};
  if (stockListSheet && stockListSheet.getLastRow() > 1) {
    const listData = stockListSheet.getRange(2, 1, stockListSheet.getLastRow() - 1, 2).getValues();
    listData.forEach(row => {
      stockNameMap[String(row[0]).trim()] = String(row[1]).trim();
    });
  }

  // ★ 2. 讀取所有交易紀錄，建立「歷史時光機」時間軸
  let txns = [];
  let existingSplits = new Set();
  const txnData = txnSheet.getDataRange().getValues();

  for (let i = 1; i < txnData.length; i++) {
    let tDate = new Date(txnData[i][1]);
    let tStockId = String(txnData[i][2]).trim();
    let tAction = txnData[i][4];
    let tUnit = txnData[i][5];
    let tQty = parseFloat(txnData[i][7]) || 0;
    let memo = String(txnData[i][9] || "");
    let tAccount = String(txnData[i][11]).trim();

    // 終極防護：過濾掉有【異常】標記的單子，不列入歷史計算
    if (memo.indexOf("【異常】") !== -1) continue; 

    // 防呆補零
    if (/^\d+$/.test(tStockId)) {
      while (tStockId.length < 4) { tStockId = "0" + tStockId; }
    }

    // 紀錄已經存在的分割里程碑
    if (tAction === "股票分割") {
      let dateStr = Utilities.formatDate(tDate, "GMT+8", "yyyy-MM-dd");
      existingSplits.add(`${tStockId}_${dateStr}_${tAccount}`);
    }

    // 將所有有效的買賣紀錄塞入時間軸
    if (tStockId && tAccount && !isNaN(tDate.getTime())) {
      txns.push({
        date: tDate,
        stockId: tStockId,
        action: tAction,
        qty: (tUnit === "整張") ? tQty * 1000 : tQty,
        account: tAccount,
        ratio: 1 
      });
    }
  }

  // ★ 3. 讀取分割設定，並放進時間軸一起排序 (改為讀取 4 欄，因為多加了 A 欄 ID)
  const splitData = splitSheet.getRange(2, 1, splitLastRow - 1, 4).getValues();
  let pendingSplits = []; 

  splitData.forEach(row => {
    let settingId = row[0];                 // A: ID
    let stockId = String(row[1]).trim();    // B: 股票代碼 (原來是 0)
    
    if (/^\d+$/.test(stockId)) {
      while (stockId.length < 4) { stockId = "0" + stockId; }
    }
    let splitDate = new Date(row[2]);       // C: 分割日期 (原來是 1)
    let ratio = parseFloat(row[3]);         // D: 分割比例 (原來是 2)

    if (stockId && !isNaN(splitDate.getTime()) && ratio > 0 && splitDate <= now) {
      txns.push({
        date: splitDate,
        stockId: stockId,
        action: 'REAL_SPLIT_EVENT',
        qty: 0,
        account: 'ALL',
        ratio: ratio
      });
      
      pendingSplits.push({
        settingId: settingId, // ★ 把設定單 ID 存起來，帶給後面的發放階段
        stockId: stockId,
        date: splitDate,
        ratio: ratio
      });
    }
  });

  // 依時間先後排序
  txns.sort((a, b) => a.date.getTime() - b.date.getTime());

  let newTxns = [];

  // ★ 4. 開始精準派發！只給「分割當下還有庫存」的帳號
  pendingSplits.forEach(split => {
    let dateStr = Utilities.formatDate(split.date, "GMT+8", "yyyy-MM-dd");
    let stockName = stockNameMap[split.stockId] || ""; 
    
    let balances = {}; // 記錄該檔股票在分割當下的各帳號庫存：{ '玉山': 1000, '國泰': 0 }
    
    // 讓時光機跑到分割日的那一刻為止
    for (let r of txns) {
      if (r.date > split.date) break; 
      
      // 遇到當下這筆分割事件就停下來（因為我們要看的是分割前的庫存）
      if (r.action === 'REAL_SPLIT_EVENT' && r.stockId === split.stockId && r.date.getTime() === split.date.getTime()) {
         break; 
      }
      
      if (r.stockId === split.stockId) {
         if (r.action === 'REAL_SPLIT_EVENT') {
            // 如果這檔股票過去曾經分割過，把庫存放大
            for (let acc in balances) {
               balances[acc] *= r.ratio;
            }
         } else {
            if (!balances[r.account]) balances[r.account] = 0;
            if (r.action === "買進" || r.action === "定期定額") {
               balances[r.account] += r.qty;
            } else if (r.action === "賣出") {
               balances[r.account] -= r.qty;
            }
         }
      }
    }

    // 檢查結算結果：該帳號的庫存是否大於 0？
    for (let account in balances) {
       if (balances[account] > 0.001) { // 確實持有庫存！
          let uniqueKey = `${split.stockId}_${dateStr}_${account}`;
          
          if (!existingSplits.has(uniqueKey)) {
             newTxns.push([
               Utilities.getUuid(),           // A: ID
               dateStr,                       // B: 交易日期
               split.stockId,                 // C: 股票代碼
               stockName,                     // D: 股票名稱
               "股票分割",                    // E: 操作項目
               "整張",                        // F: 單位
               0,                             // G: 價格
               0,                             // H: 數量
               "",                            // I: 證券商
               `✨ 執行 1 拆 ${split.ratio} (庫存與成本已由系統結算)`, // J: 備註
               0,                             // K: 交割金額
               account,                       // L: 帳號
               false,                         // M: 本期含股利再投入
               0,                             // N: 投入股利金額
               split.settingId                // O: 父單ID (★成功綁定股票分割單 ID)
             ]);
             existingSplits.add(uniqueKey);
          }
       }
    }
  });

  // ★ 5. 寫入 Transactions (範圍改為 15 欄)
  if (newTxns.length > 0) {
    txnSheet.getRange(txnSheet.getLastRow() + 1, 1, newTxns.length, 15).setValues(newTxns);
    logToDebug(`✅ 成功寫入 ${newTxns.length} 筆股票分割里程碑紀錄！`);
  }
}

// ★★★ 功能：買賣價格防呆驗證 (專供 AppSheet Bot 呼叫) ★★★
function validateStockPrice(recordId, stockId, inputPrice, dateStr, unit) { // ★ 新增 unit 參數
  try {
    const ss = getSS();
    let helperSheet = ss.getSheetByName("Helper");
    if (!helperSheet) {
      helperSheet = ss.insertSheet("Helper"); // 建立隱藏的計算分頁
    }
    
    // 處理日期格式 (將 yyyy-MM-dd 拆解)
    let d = new Date(dateStr);
    let y = d.getFullYear();
    let m = d.getMonth() + 1;
    let day = d.getDate();
    
    let tpeCode = "TPE:" + stockId;
    let twoCode = "TWO:" + stockId;
    
    // 寫入 GOOGLEFINANCE 公式查詢「當日最高價」與「最低價」
    let fHigh = `=IFERROR(INDEX(GOOGLEFINANCE("${tpeCode}", "high", DATE(${y},${m},${day})), 2, 2), IFERROR(INDEX(GOOGLEFINANCE("${twoCode}", "high", DATE(${y},${m},${day})), 2, 2), 0))`;
    let fLow = `=IFERROR(INDEX(GOOGLEFINANCE("${tpeCode}", "low", DATE(${y},${m},${day})), 2, 2), IFERROR(INDEX(GOOGLEFINANCE("${twoCode}", "low", DATE(${y},${m},${day})), 2, 2), 0))`;
    
    helperSheet.getRange("A1").setFormula(fHigh);
    helperSheet.getRange("B1").setFormula(fLow);
    SpreadsheetApp.flush(); // 強制試算表立刻計算
    
    // 等待 Google Finance 載入資料 (因網路讀取，最多等 5 秒)
    let high = 0, low = 0;
    for (let i = 0; i < 5; i++) {
      Utilities.sleep(1000);
      let valHigh = helperSheet.getRange("A1").getValue();
      let valLow = helperSheet.getRange("B1").getValue();
      if (valHigh !== "#N/A" && valHigh !== "Loading..." && valHigh !== "") {
        high = parseFloat(valHigh) || 0;
        low = parseFloat(valLow) || 0;
        break;
      }
    }
    
    // 如果查無資料 (例如週末假日、或尚未開盤)，則放行不攔截
    if (high === 0 || low === 0) {
      generateInventoryReport(); // ★ 放行也要確保庫存更新
      return;
    }

    inputPrice = parseFloat(inputPrice);
    
    // ★★★ 新增：零股寬容區間邏輯 ★★★
    let maxValidPrice = high;
    let minValidPrice = low;
    let isFractional = (unit === "零股"); // 判斷是否為零股

    if (isFractional) {
      // 零股容許上下 3% 的溢價與折價空間 (1.03 與 0.97 可自由調整)
      maxValidPrice = high * 1.03;
      minValidPrice = low * 0.97;
    }
    
    // ⚠️ 判斷是否異常：使用新的容許區間來比對
    if (inputPrice > maxValidPrice || inputPrice < minValidPrice) {
      const txnSheet = ss.getSheetByName("Transactions");
      const data = txnSheet.getDataRange().getValues();
      
      for (let i = 1; i < data.length; i++) {
        if (data[i][0] === recordId) { 
          let currentNote = data[i][9]; 
          
          // 將顯示區間四捨五入到小數點後兩位，讓畫面乾淨
          let rangeStr = `${minValidPrice.toFixed(2)} ~ ${maxValidPrice.toFixed(2)}`;
          let warnMsg = `【異常】輸入價 ${inputPrice} 超出當日區間 (${rangeStr}) →異常單不計入庫存計算。`;
          
          if (String(currentNote).indexOf("【異常】") === -1) {
            let newNote = currentNote ? currentNote + "\n" + warnMsg : warnMsg;
            txnSheet.getRange(i + 1, 10).setValue(newNote); 
            logToDebug(`⚠️ 攔截異常價格：${stockId} 於 ${dateStr} 輸入 ${inputPrice} (單位: ${unit})`);

            // ★ 更新：Email 內容加入單位與寬容值提示
            try {
              let userEmail = Session.getEffectiveUser().getEmail(); 
              let subject = `⚠️ 【股票系統警報】${stockId} 價格輸入異常`;
              let body = `系統偵測到您的買賣紀錄出現異常價格！\n\n` +
                         `📌 交易日期：${dateStr}\n` +
                         `📌 股票代碼：${stockId}\n` +
                         `📌 交易單位：${unit}\n` +
                         `📌 輸入價格：${inputPrice} 元\n` +
                         `📌 當日合理區間：${rangeStr} 元 ${isFractional ? "(已包含零股 3% 寬容值)" : ""}\n\n` +
                         `該筆紀錄已自動標記為【異常】並從庫存計算中隔離。\n` +
                         `請盡快登入系統確認並修正為正確價格。`;
              
              MailApp.sendEmail(userEmail, subject, body);
              logToDebug("📧 異常通知信已成功發送至: " + userEmail);
            } catch (mailErr) {
              logToDebug("❌ 信件發送失敗: " + mailErr.toString());
            }
          }
          break;
        }
      }
    }
    
    // ★ 終極殺招：無論這筆單有沒有異常，防呆檢查完畢後一律強制重新計算庫存！
    generateInventoryReport();

  } catch (e) {
    logToDebug("防呆驗證發生錯誤: " + e.toString());
  }
}
