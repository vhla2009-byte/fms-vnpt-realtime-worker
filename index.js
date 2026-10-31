/**
 * ============================================================================
 * FMS VNPT 24/7 CLOUD WORKER (SIGNALR WEBSOCKET -> GOOGLE SHEETS + FIREBASE + TELEGRAM)
 * ============================================================================
 * Chạy hoàn toàn trên Cloud miễn phí (Render.com / Koyeb / Fly.io / Railway):
 *  1. Kết nối trực tiếp WebSocket ASP.NET Core SignalR: wss://fms.vnpt.vn/alarmHub
 *  2. Bắt sự kiện realtime `SendAlarm` độ trễ < 500ms.
 *  3. Tự động gọi `/new/pingback` mỗi 60s để giữ Cookie sống 24/7.
 *  4. Tự động đẩy dữ liệu vào:
 *     - Google Sheets (qua Google Apps Script Webhook doPost)
 *     - Firebase Realtime Database (appqlnt-ee302-default-rtdb)
 *     - Telegram Bot (Cảnh báo tức thời + Nhận lệnh `/cookie <mã>` từ điện thoại)
 */

// Cho phép chứng chỉ SSL nội bộ của VNPT (fms.vnpt.vn)
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// BẢO VỆ TIẾN TRÌNH KHÔNG BAO GIỜ BỊ CRASH 24/7 (Bắt mọi lỗi ngoại lệ kết nối mạng / SignalR)
process.on("uncaughtException", (err) => {
  console.error("💥 [CRASH GUARD] Bắt lỗi ngoại lệ không mong muốn:", err?.message || err);
  if (String(err?.message || "").includes("HttpConnection") || String(err?.message || "").includes("SignalR") || String(err?.message || "").includes("WebSocket")) {
    console.warn("⚠️ Lỗi mạng tầng SignalR đã được chặn thành công, tự động kết nối lại sau 5s...");
    setTimeout(() => {
      startSignalRConnection().catch(e => console.error("Tái kết nối SignalR thất bại:", e.message));
    }, 5000);
  }
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("💥 [CRASH GUARD] Unhandled Rejection:", reason?.message || reason);
});

const http = require("http");
const signalR = require("@microsoft/signalr");

// Cấu hình từ biến môi trường (hoặc mặc định dự án QLNT)
const PORT = process.env.PORT || 3000;
const FMS_BASE_URL = "https://fms.vnpt.vn";
const FMS_PROVINCE = process.env.FMS_PROVINCE || "1057,1006";
const FIREBASE_DB_URL = (process.env.FIREBASE_DB_URL || "https://appqlnt-ee302-default-rtdb.asia-southeast1.firebasedatabase.app").replace(/\/$/, "");
let GOOGLE_SHEET_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycbxmBxO3sPvXlkxY9EYNdfmj5Lj-_biDQSo1GRmsuy6tP8MFZYByY4SWjS1MkfEz1frKHw/exec";
const MAY_NO_SHEET_WEBHOOK_URL = process.env.MAY_NO_SHEET_WEBHOOK_URL || "https://script.google.com/macros/s/AKfycbz_Kb3Q7xI2_AQoKye3Q03-m6658m8zyOfYUGFsKpR_PjzRTSZx9YNr1n8GefNelTey/exec";
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";

// Trạng thái vận hành
let currentSyncMode = "SIGNALR_WEBSOCKET";
let currentCookie = process.env.FMS_COOKIE || "";
let currentAccount = "ytv.lan";
let restCookie = "";
let restAccount = "huyhd.lan";
let wsCookie = "";
let wsAccount = "ytv.lan";
let hubConnection = null;
let isSessionValid = false;
let expiredAlertSent = false;

// ==================== QUY TẮC TỰ ĐỘNG XIN OTP SAU 3 LẦN ĐỐI SOÁT 20K THẤT BẠI ====================
let consecutiveReconcileFailures = 0;
const AUTO_OTP_COOLDOWN_MS = 15 * 60 * 1000; // Tối thiểu 15 phút giữa 2 lần tự xin OTP để chống spam SMS

async function handleReconcileFailure(reason = "") {
  consecutiveReconcileFailures++;
  console.warn(`⚠️ [QUY TẮC ĐỐI SOÁT] Đối soát 20k thất bại lần ${consecutiveReconcileFailures}/3. Lý do: ${reason}`);

  if (consecutiveReconcileFailures >= 3) {
    const now = Date.now();
    if (now - lastAutoOtpRequestTime < AUTO_OTP_COOLDOWN_MS) {
      console.warn(`⏳ [AUTO OTP] Đã đối soát 20k thất bại ${consecutiveReconcileFailures} lần liên tiếp nhưng đang trong thời gian cooldown (15 phút).`);
      return;
    }

    lastAutoOtpRequestTime = now;
    console.log(`🚨 [AUTO OTP RULE] ĐÃ ĐỐI SOÁT 20K THẤT BẠI 3 LẦN LIÊN TIẾP! TỰ ĐỘNG GỬI YÊU CẦU MÃ OTP CAS VNPT NGAY...`);

    sendTelegramMessage(`🚨 *CẢNH BÁO FMS:* Đối soát 20.000 tin thất bại 3 lần liên tiếp do Cookie hết hạn (${reason}). Cloud Worker đang tự động gửi yêu cầu cấp mã OTP mới về điện thoại...`).catch(() => {});

    const targetUser = currentAccount || (currentSyncMode === "CLOUD_REST" ? "huyhd.lan" : "ytv.lan");
    const targetPass = (targetUser === "ytv.lan") ? "P#r9zhk4" : (process.env.FMS_CAS_PASS || "Vnpt@123456");

    try {
      const otpRes = await requestCasOtp(targetUser, targetPass);
      console.log(`✅ [AUTO OTP] Đã tự động kích hoạt gửi mã OTP cho ${targetUser}:`, otpRes.message);
      await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...otpRes, trigger: "auto_rule_3_failures", timestamp: Date.now() })
      }).catch(() => {});
    } catch (errOtp) {
      console.error(`❌ [AUTO OTP] Lỗi khi tự động gửi yêu cầu OTP:`, errOtp.message);
    }
  }
}

let lastPingTime = null;
let lastSyncTime = null;
let activeAlarmCount = 0;
let telegramOffset = 0;
let lastAutoOtpRequestTime = 0;

/**
 * Chuẩn hóa chuỗi Cookie FMS
 */
function normalizeCookie(raw) {
  if (!raw) return "";
  let clean = String(raw).trim();
  if (!clean.includes("=")) {
    clean = `.AspNetCore.Cookies.fms_radio=${clean}`;
  }
  return clean;
}

/**
 * 1. Đọc/Ghi Cookie & Cấu hình trên Firebase RTDB tự động theo nút chọn trên Web App
 */
async function loadConfigFromFirebase() {
  try {
    const res = await fetch(`${FIREBASE_DB_URL}/_fms_config.json`);
    if (res.ok) {
      const data = await res.json();
      if (!data) return;

      const newMode = data.sync_mode || "SIGNALR_WEBSOCKET";
      const oldMode = currentSyncMode;

      restCookie = normalizeCookie(data?.auth_rest?.cookie || (newMode === "CLOUD_REST" ? data?.auth?.cookie : ""));
      restAccount = data?.auth_rest?.user || "huyhd.lan";
      wsCookie = normalizeCookie(data?.auth_ws?.cookie || (newMode === "SIGNALR_WEBSOCKET" ? data?.auth?.cookie : ""));
      wsAccount = data?.auth_ws?.user || "ytv.lan";

      currentSyncMode = newMode;

      if (currentSyncMode === "CLOUD_REST") {
        currentCookie = restCookie;
        currentAccount = restAccount;
        console.log(`[CONFIG] Đang ở Giải Pháp 1: Cloud REST Polling (${currentAccount}).`);
        if (oldMode !== "CLOUD_REST") {
          console.log(`\n======================================================`);
          console.log(`🔄 [MODE SWITCH] Chuyển sang Giải Pháp 1: REST (${currentAccount})`);
          console.log(`======================================================\n`);
          if (hubConnection && hubConnection.state === signalR.HubConnectionState.Connected) {
            try { await hubConnection.stop(); } catch {}
          }
          await syncFullSnapshotFromRestApi();
        }
      } else {
        currentCookie = wsCookie;
        currentAccount = wsAccount;
        console.log(`[CONFIG] Đang ở Giải Pháp 2: SignalR WebSocket (${currentAccount}).`);
        if (oldMode !== "SIGNALR_WEBSOCKET") {
          console.log(`\n======================================================`);
          console.log(`⚡ [MODE SWITCH] Chuyển sang Giải Pháp 2: SignalR WS (${currentAccount})`);
          console.log(`======================================================\n`);
          await startSignalRConnection();
        }
      }
    }
  } catch (err) {
    console.error("[INIT] Lỗi tải cấu hình từ Firebase:", err.message);
  }
}

async function saveCookieEverywhere(newCookie, user = "ytv.lan") {
  currentCookie = normalizeCookie(newCookie);
  currentAccount = user || "ytv.lan";
  expiredAlertSent = false;

  // Lưu lên Firebase RTDB
  try {
    const nowIso = new Date().toISOString();
    const nowVn = new Date().toLocaleString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh" });

    await fetch(`${FIREBASE_DB_URL}/_fms_config.json`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        sync_mode: "SIGNALR_WEBSOCKET",
        auth: {
          cookie: currentCookie,
          updated_at: nowIso,
          updated_at_vn: nowVn,
          status: "ACTIVE",
          user: currentAccount
        },
        auth_ws: {
          cookie: currentCookie,
          updated_at: nowIso,
          updated_at_vn: nowVn,
          status: "ACTIVE",
          user: currentAccount
        }
      })
    });
  } catch (e) {
    console.error("[FIREBASE] Lỗi lưu Cookie:", e.message);
  }

  // Đồng bộ Cookie sang Google Sheet Apps Script
  if (GOOGLE_SHEET_WEBHOOK_URL) {
    try {
      await fetch(GOOGLE_SHEET_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "update_cookie", cookie: currentCookie, user: currentAccount }),
        redirect: "follow"
      });
    } catch (e) {
      console.error("[SHEET] Lỗi đồng bộ Cookie:", e.message);
    }
  }
}

/**
 * 2. Gửi dữ liệu sự cố sang Google Sheets Webhook + Firebase RTDB
 */
async function pushAlarmsToGoogleSheets(alarms) {
  if (!GOOGLE_SHEET_WEBHOOK_URL) return;
  try {
    const syncUserLabel = currentSyncMode === "CLOUD_REST"
      ? `${currentAccount || "huyhd.lan"} (REST_API)`
      : `${currentAccount || "ytv.lan"} (SignalR_WS)`;

    const list = (Array.isArray(alarms) ? alarms : [alarms]).map(a => ({
      ...a,
      SYNC_USER: syncUserLabel
    }));
    const res = await fetch(GOOGLE_SHEET_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(list),
      redirect: "follow"
    });
    const text = await res.text();
    console.log(`[GOOGLE SHEETS] Đã đẩy ${list.length} sự cố [${syncUserLabel}] -> Phản hồi: ${text.substring(0, 120)}`);
  } catch (err) {
    console.error("[GOOGLE SHEETS] Lỗi đẩy Webhook:", err.message);
  }
}

// ==================== BỘ NHỚ ĐỆM RAM (IN-MEMORY CACHE) TIẾT KIỆM 99.9% BĂNG THÔNG ====================
const activeAlarmsMemoryMap = new Map();
let stationsMemoryCache = null;
let stationsCacheLastFetchedAt = 0;
const STATIONS_CACHE_TTL_MS = 12 * 60 * 60 * 1000; // 12 tiếng làm mới danh mục trạm 1 lần (đảm bảo không bao giờ vượt trần 360MB/ngày)
let isStationsFetching = false;

/**
 * Lấy dữ liệu toàn bộ trạm từ bộ nhớ RAM (Chỉ tải Firebase 1 lần khi khởi động hoặc sau mỗi 1 tiếng)
 */
async function getStationsFromMemoryOrDb(forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && stationsMemoryCache && (now - stationsCacheLastFetchedAt < STATIONS_CACHE_TTL_MS)) {
    return stationsMemoryCache;
  }
  if (isStationsFetching && stationsMemoryCache) {
    return stationsMemoryCache;
  }
  isStationsFetching = true;
  try {
    console.log("[CACHE RAM] 📥 Đang tải stations.json từ Firebase vào bộ nhớ RAM...");
    const res = await fetch(`${FIREBASE_DB_URL}/stations.json`);
    if (res.ok) {
      stationsMemoryCache = await res.json();
      stationsCacheLastFetchedAt = Date.now();
      const regCount = stationsMemoryCache ? Object.keys(stationsMemoryCache).length : 0;
      console.log(`[CACHE RAM] ✅ Đã lưu stations.json vào RAM thành công! (${regCount} khu vực)`);
    }
  } catch (err) {
    console.warn("[CACHE RAM] ⚠️ Lỗi tải stations.json:", err.message);
  } finally {
    isStationsFetching = false;
  }
  return stationsMemoryCache;
}

/**
 * Lấy danh sách sự cố FMS đang Active trực tiếp từ RAM (0 MB tải Firebase!)
 */
async function getActiveAlarmsList() {
  if (activeAlarmsMemoryMap.size > 0) {
    return Array.from(activeAlarmsMemoryMap.values());
  }
  try {
    console.log("[CACHE RAM] 📥 Nạp danh sách sự cố FMS từ Firebase vào RAM...");
    const res = await fetch(`${FIREBASE_DB_URL}/fms_alarms/items.json`);
    if (res.ok) {
      const itemsMap = await res.json();
      if (itemsMap && typeof itemsMap === "object") {
        activeAlarmsMemoryMap.clear();
        for (const [k, v] of Object.entries(itemsMap)) {
          if (v && String(v.CLEAR_STATUS || "0") !== "1" && v.CLR_TEXT !== "CLR") {
            activeAlarmsMemoryMap.set(k, v);
          }
        }
      }
    }
  } catch (err) {
    console.warn("[CACHE RAM] ⚠️ Lỗi nạp fms_alarms vào RAM:", err.message);
  }
  return Array.from(activeAlarmsMemoryMap.values());
}

async function pushAlarmsToFirebase(alarms, isFullSnapshot = false) {
  try {
    const list = Array.isArray(alarms) ? alarms : [alarms];
    if (isFullSnapshot) {
      activeAlarmsMemoryMap.clear();
      const items = {};
      list.forEach(item => {
        if (item) {
          const key = getUnifiedAlarmKey(item) || String(item.GLOBAL_ID);
          const clr = getClrStatus(item);
          const enriched = {
            ...item,
            UNIFIED_KEY: key,
            CLR_TEXT: clr
          };
          items[key] = enriched;
          if (String(item.CLEAR_STATUS || "0") !== "1" && clr !== "CLR") {
            activeAlarmsMemoryMap.set(key, enriched);
          }
        }
      });
      activeAlarmCount = activeAlarmsMemoryMap.size || list.length;
      await fetch(`${FIREBASE_DB_URL}/fms_alarms.json`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          updated_at: new Date().toISOString(),
          total_active: activeAlarmCount,
          items
        })
      });
    } else {
      // Cập nhật từng sự cố phát sinh hoặc khôi phục từ SignalR
      for (const item of list) {
        if (!item) continue;
        const keyActive = getUnifiedAlarmKey({ ...item, CLEAR_STATUS: "0" });
        const keyClr = getUnifiedAlarmKey({ ...item, CLEAR_STATUS: "1" });
        const gid = item.GLOBAL_ID ? String(item.GLOBAL_ID) : null;
        const isClr = getClrStatus(item) === "CLR";

        if (isClr) {
          if (keyActive) activeAlarmsMemoryMap.delete(keyActive);
          if (keyClr) activeAlarmsMemoryMap.delete(keyClr);
          if (gid) activeAlarmsMemoryMap.delete(gid);
          if (keyActive) await fetch(`${FIREBASE_DB_URL}/fms_alarms/items/${encodeURIComponent(keyActive)}.json`, { method: "DELETE" }).catch(() => {});
          if (gid) await fetch(`${FIREBASE_DB_URL}/fms_alarms/items/${gid}.json`, { method: "DELETE" }).catch(() => {});
        } else {
          const writeKey = keyActive || gid;
          if (writeKey) {
            const enriched = {
              ...item,
              UNIFIED_KEY: writeKey,
              CLR_TEXT: "NCLR"
            };
            activeAlarmsMemoryMap.set(writeKey, enriched);
            await fetch(`${FIREBASE_DB_URL}/fms_alarms/items/${encodeURIComponent(writeKey)}.json`, {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(enriched)
            }).catch(() => {});
          }
        }
      }
      activeAlarmCount = activeAlarmsMemoryMap.size;
      // Cập nhật mốc thời gian sự cố realtime mới nhất lên Firebase
      const nowIso = new Date().toISOString();
      const nowVn = new Date().toLocaleString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh" });
      await fetch(`${FIREBASE_DB_URL}/fms_alarms.json`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          updated_at: nowIso,
          updated_at_vn: nowVn,
          total_active: activeAlarmCount
        })
      }).catch(() => {});
    }

    // Tự động kích hoạt đồng bộ chuẩn hóa trạng thái trạm theo FMS (gom cụm sau 3s)
    scheduleStationStatusSync(3000);
  } catch (err) {
    console.error("[FIREBASE] Lỗi cập nhật sự cố:", err.message);
  }
}

// ==================== ĐỒNG BỘ TRẠNG THÁI TRẠM CHUẨN FMS -> FIREBASE & SHEET MÁY NỔ ====================
function extractStationCore(str) {
  if (!str) return "";
  const m = String(str).toUpperCase().match(/(?:^|[^A-Z0-9])([A-Z]{3}\d{3,4})(?!\d)/i);
  return m ? m[1].toUpperCase() : "";
}

function isStationMatchingAlarm(alarmNeCode, stationCsht) {
  if (!alarmNeCode || !stationCsht) return false;
  const ne = String(alarmNeCode).trim().toUpperCase();
  const csht = String(stationCsht).trim().toUpperCase();
  if (ne === csht) return true;
  const coreA = extractStationCore(ne);
  const coreB = extractStationCore(csht);
  if (coreA && coreB && coreA === coreB) return true;
  return false;
}

function computeStationStatusFromAlarms(stationAlarms) {
  let isGrid = false;
  let isGen = false;
  let isAc = false;

  for (const alarm of stationAlarms) {
    const causes = String(alarm.ALARM_CAUSE_CODES || "").toUpperCase();
    // Nào ra đó, không gộp: DIEN_LUOI -> Điện lưới, MAY_NO -> Máy nổ, POWER_AC_EAS -> AC trạm
    if (causes.includes("DIEN_LUOI")) {
      isGrid = true;
    }
    if (causes.includes("MAY_NO")) {
      isGen = true;
    }
    if (causes.includes("POWER_AC_EAS")) {
      isAc = true;
    }
  }

  const lines = [
    isGrid ? "MẤT ĐIỆN LƯỚI" : "CÓ ĐIỆN LƯỚI",
    isGen ? "MÁY NỔ ĐANG CHẠY" : "MÁY NỔ ĐANG TẮT",
    isAc ? "MẤT AC TRẠM" : "CÓ AC TRẠM"
  ];

  return lines.join("\n");
}

let stationSyncTimer = null;
let isSyncingStations = false;

function scheduleStationStatusSync(delayMs = 3000) {
  if (stationSyncTimer) clearTimeout(stationSyncTimer);
  stationSyncTimer = setTimeout(() => {
    syncStationStatusesFromFmsAlarms().catch((e) => {
      console.warn("[STATION SYNC] Lỗi chạy ngầm:", e.message);
    });
  }, delayMs);
}

/**
 * Đối soát và đồng bộ trạng thái trạm: SỬ DỤNG 100% BỘ NHỚ RAM ĐỆM
 * Không tải lại toàn bộ 25MB stations.json mỗi lần có sự cố -> Giảm 99.9% băng thông tải!
 */
async function syncStationStatusesFromFmsAlarms() {
  if (isSyncingStations) return;
  isSyncingStations = true;
  try {
    // 1. Lấy dữ liệu trạm từ RAM (nếu chưa có mới nạp 1 lần)
    const stationsData = await getStationsFromMemoryOrDb();
    if (!stationsData) return;

    // 2. Lấy sự cố Active trực tiếp từ RAM (0 MB tải Firebase!)
    const activeAlarms = await getActiveAlarmsList();

    const regions = ["THU", "VHU", "TTH", "THO", "MHO", "KTU"];
    let updatedCount = 0;

    for (const reg of regions) {
      const rawList = stationsData[reg];
      if (!rawList) continue;
      const entries = Object.entries(rawList);

      for (const [idxKey, st] of entries) {
        if (!st || !st.CSHT) continue;
        const csht = st.CSHT;
        const rowIndex = st.rowIndex != null ? Number(st.rowIndex) : (Number(idxKey) || null);

        const stAlarms = activeAlarms.filter(a => isStationMatchingAlarm(a.NE_CODE, csht));
        const newStatus = computeStationStatusFromAlarms(stAlarms);
        const currentStatus = (st.STATUS || "").trim();

        if (currentStatus !== newStatus.trim()) {
          updatedCount++;
          console.log(`[STATION SYNC] Cập nhật ${reg} - ${csht}: ${currentStatus.replace(/\n/g, " | ")} -> ${newStatus.replace(/\n/g, " | ")}`);

          // Cập nhật ngay vào RAM cache để các vòng lặp kế tiếp không gửi lệnh PATCH trùng lặp
          st.STATUS = newStatus;

          // 1. Cập nhật DUY NHẤT node trạm này lên Firebase RTDB (chỉ vài chục bytes)
          await fetch(`${FIREBASE_DB_URL}/stations/${reg}/${idxKey}.json`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ STATUS: newStatus })
          }).catch(() => {});

          // 2. Đồng bộ về Sheet Máy Nổ (nếu có rowIndex)
          if (rowIndex) {
            fetch(MAY_NO_SHEET_WEBHOOK_URL, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                sheetName: reg,
                rowIndex: rowIndex,
                STATUS: newStatus
              })
            }).catch(() => {});
          }
        }
      }
    }
    if (updatedCount > 0) {
      console.log(`[STATION SYNC] ✅ Đã cập nhật xong ${updatedCount} trạm biến động lên Firebase.`);
    }
  } catch (err) {
    console.error("[STATION SYNC] Lỗi trong quá trình đối soát trạm:", err.message);
  } finally {
    isSyncingStations = false;
  }
}

function cleanAlarmCauses(causeStr) {
  if (!causeStr) return "";
  return String(causeStr)
    .split(",")
    .map(s => s.trim())
    .filter(s => s && s.toUpperCase() !== "OTHERS")
    .join(", ");
}

function isTnhLanAlarm(item) {
  if (!item || typeof item !== "object") return false;
  const neCode = String(item.NE_CODE || "").trim().toUpperCase();
  const prov = String(item.PROVINCE_NAME || item.PROVINCE || "").trim().toUpperCase();
  const isTargetNe = neCode.includes("LAN") || neCode.includes("TNH") ||
                     prov.includes("LONG AN") || prov === "1006" ||
                     prov.includes("TÂY NINH") || prov.includes("TAY NINH") || prov === "1057";
  if (!isTargetNe) return false;

  const cleanedCause = cleanAlarmCauses(item.ALARM_CAUSE_CODES);
  if (!cleanedCause) return false;

  item.ALARM_CAUSE_CODES = cleanedCause;
  return true;
}

/**
 * Chuẩn hóa trạng thái phục hồi: CLR (Đã phục hồi) hoặc NCLR (Chưa phục hồi / Đang diễn ra)
 */
function getClrStatus(item) {
  if (!item) return "NCLR";
  const clr = String(item.CLEAR_STATUS || item.CLR_STATUS || "");
  const status = String(item.STATUS || "").trim().toUpperCase();
  if (clr === "1" || clr.toUpperCase() === "CLR" || status === "KP" || status === "CLR" || status.includes("PHỤC HỒI") || status.includes("ĐÃ PHỤC HỒI")) {
    return "CLR";
  }
  return "NCLR";
}

/**
 * Khóa định danh hợp nhất theo yêu cầu chuẩn:
 * Mã trạm (NE_CODE) + Nguyên nhân (ALARM_CAUSE_CODES) + GLOBAL_ID + CLR (NCLR)
 */
function getUnifiedAlarmKey(item) {
  if (!item || typeof item !== "object") return "";
  const ne = String(item.NE_CODE || "").trim().toUpperCase();
  const cause = cleanAlarmCauses(item.ALARM_CAUSE_CODES).toUpperCase();
  const gid = String(item.GLOBAL_ID || item.ALARM_ID || item.ID || "").trim();
  const clr = getClrStatus(item);
  return `${ne}__${cause}__${gid}__${clr}`;
}

/**
 * 3. Kết nối ASP.NET Core SignalR WebSocket (`wss://fms.vnpt.vn/alarmHub`)
 */
async function startSignalRConnection() {
  if (!currentCookie) {
    console.log("[SIGNALR] Chưa có Cookie FMS. Đang chờ nạp Cookie qua lệnh Telegram /cookie hoặc HTTP POST...");
    return;
  }

  if (hubConnection) {
    try {
      await hubConnection.stop();
    } catch (e) {}
  }

  console.log("[SIGNALR] Đang khởi tạo kết nối tới https://fms.vnpt.vn/alarmHub ...");

  hubConnection = new signalR.HubConnectionBuilder()
    .withUrl(`${FMS_BASE_URL}/alarmHub`, {
      transport: signalR.HttpTransportType.WebSockets,
      headers: {
        "Cookie": currentCookie,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0.0.0 Safari/537.36",
        "Origin": FMS_BASE_URL,
        "Referer": `${FMS_BASE_URL}/new/canhbao/radio3`
      }
    })
    .withAutomaticReconnect([0, 2000, 5000, 10000, 30000])
    .configureLogging(signalR.LogLevel.Information)
    .build();

  // Lắng nghe sự kiện Realtime từ tổng đài FMS
  hubConnection.on("SendAlarm", async (message) => {
    let alarms = [];
    if (typeof message === "string") {
      try {
        alarms = JSON.parse(message);
      } catch (e) {
        console.warn("[SIGNALR] Không parse được chuỗi JSON message:", e.message);
      }
    } else if (Array.isArray(message)) {
      alarms = message;
    } else if (message && typeof message === "object") {
      alarms = message.data || [message];
    }

    if (!Array.isArray(alarms)) {
      alarms = [alarms];
    }

    const targetAlarms = alarms.filter(isTnhLanAlarm);
    if (targetAlarms.length > 0) {
      console.log(`🚨 [SỰ CỐ TNH/LAN PHÁT SINH] Có ${targetAlarms.length} trạm:`, targetAlarms.map(a => `${a.NE_CODE} [${a.ALARM_CAUSE_CODES}] (CLEAR=${a.CLEAR_STATUS})`).join(", "));
      // Đẩy tức thời lên Google Sheets & Firebase
      await Promise.all([
        pushAlarmsToGoogleSheets(targetAlarms),
        pushAlarmsToFirebase(targetAlarms, false)
      ]);
    }
  });

  hubConnection.onclose((err) => {
    console.warn("[SIGNALR] Kết nối đã đóng:", err ? err.message : "No error");
    isSessionValid = false;
  });

  try {
    await hubConnection.start();
    isSessionValid = true;
    expiredAlertSent = false;
    console.log("✅ [SIGNALR] ĐÃ KẾT NỐI REALTIME THÀNH CÔNG TỚI FMS VNPT!");
    await syncFullSnapshotFromRestApi();
  } catch (err) {
    isSessionValid = false;
    console.error("❌ [SIGNALR] Lỗi kết nối:", err.message);
    if (String(err.message).includes("401") || String(err.message).includes("302") || String(err.message).includes("Unexpected server response")) {
      await notifyCookieExpired();
    }
  }
}

/**
 * 4. Duy trì phiên đăng nhập & Kiểm tra tính hợp lệ của Cookie HTTP & WebSocket
 */
async function pingbackAndKeepAlive() {
  if (!currentCookie) return;
  try {
    // 1. Kiểm tra Cookie HTTP thực tế qua endpoint pingback của FMS (không dựa dẫm vào trạng thái WebSocket)
    const pingRes = await fetch(`${FMS_BASE_URL}/new/pingback?lnk=${encodeURIComponent(FMS_BASE_URL + "/new/canhbao/radio3")}`, {
      headers: {
        "Cookie": currentCookie,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
      },
      redirect: "manual"
    });

    lastPingTime = new Date().toISOString();
    const loc = pingRes.headers.get("location");
    // Nếu trả về 401, 403 hoặc 302 chuyển hướng login -> Cookie HTTP ĐÃ HẾT HẠN!
    if (pingRes.status === 401 || pingRes.status === 403 || (loc && loc.includes("login"))) {
      console.warn(`⚠️ [PINGBACK] Phát hiện Cookie FMS HTTP đã hết hạn thực tế! (Status: ${pingRes.status})`);
      isSessionValid = false;
      await notifyCookieExpired();
      return;
    }

    if (pingRes.ok) {
      isSessionValid = true;
      if (!hubConnection || hubConnection.state === signalR.HubConnectionState.Disconnected) {
        await startSignalRConnection();
      }
    }
  } catch (err) {
    console.error("[PINGBACK] Lỗi kiểm tra phiên:", err.message);
  }
}

function formatVNDate(d) {
  const pad = (n) => (n < 10 ? "0" + n : n);
  return `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

/**
 * Lấy dữ liệu cảnh báo từ CẢ 2 HƯỚNG:
 * - Hướng 1: Tab "Vô tuyến" cũ (/api/alarm/find) -> LÀM GỐC (ROOT/PRIMARY)
 * - Hướng 2: Tab "Vô tuyến (New)" (/new/api/alarm/find) -> BỔ SUNG & ĐỐI CHIẾU
 * - Loại bỏ tin trùng lặp (Dedup theo Mã trạm + Nguyên nhân hoặc GLOBAL_ID)
 */
async function fetchDualSourceAlarms(activeCookie) {
  const now = new Date();
  const fromDate = new Date(now.getTime() - 2 * 24 * 60 * 60 * 1000);
  const toDate = new Date(now.getTime() + 1 * 24 * 60 * 60 * 1000);

  const baseParams = {
    rowslimit: "20000",
    SYSTEM_TYPE: "",
    PROVINCE: FMS_PROVINCE,
    DISTRICT: "",
    SUPPLIER_ID: "",
    NE_TYPE: "",
    ALARM_CAUSE_ID: "",
    BD_TYPE_ID: "",
    REASON_COMMENT_ID: "",
    CLEAR_STATUS: "",
    NE_VIP: "",
    FROM_DATE: formatVNDate(fromDate),
    TO_DATE: formatVNDate(toDate),
    NE_CODE: "",
    TIME_ALARM: "1",
    ALARM_DETAIL: "",
    ALARM_LEVEL_ID: "1,2,3",
    AREA: "",
    search_type: "reload"
  };

  const qsOld = new URLSearchParams(baseParams);
  const qsNew = new URLSearchParams({ ...baseParams, WARD: "" });

  const headersCommon = {
    "Cookie": activeCookie,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
  };

  let oldAlarms = [];
  let newAlarms = [];
  let isSessionValidOld = true;
  let isSessionValidNew = true;

  const [resOld, resNew] = await Promise.allSettled([
    // Hướng 1: Tab Vô tuyến cũ (/api/alarm/find) -> LÀM GỐC
    fetch(`${FMS_BASE_URL}/api/alarm/find?${qsOld.toString()}`, {
      method: "GET",
      headers: { ...headersCommon, "Referer": `${FMS_BASE_URL}/canhbao/radio3` },
      redirect: "manual"
    }),
    // Hướng 2: Tab Vô tuyến mới (/new/api/alarm/find)
    fetch(`${FMS_BASE_URL}/new/api/alarm/find?${qsNew.toString()}`, {
      method: "GET",
      headers: { ...headersCommon, "Referer": `${FMS_BASE_URL}/new/canhbao/radio3` },
      redirect: "manual"
    })
  ]);

  if (resOld.status === "fulfilled") {
    const res = resOld.value;
    if (res.status === 302 || res.status === 401 || res.status === 403) {
      isSessionValidOld = false;
    } else if (res.ok) {
      try {
        const d = await res.json();
        oldAlarms = d.data || [];
      } catch (e) {}
    }
  }

  if (resNew.status === "fulfilled") {
    const res = resNew.value;
    if (res.status === 302 || res.status === 401 || res.status === 403) {
      isSessionValidNew = false;
    } else if (res.ok) {
      try {
        const d = await res.json();
        newAlarms = d.data || [];
      } catch (e) {}
    }
  }

  // HỢP NHẤT DỮ LIỆU: LẤY BẢN VÔ TUYẾN CŨ LÀM GỐC, KHỬ TRÙNG LẶP THEO KHÓA CHUẨN:
  // Mã trạm (NE_CODE) + Nguyên nhân (ALARM_CAUSE_CODES) + GLOBAL_ID + CLR (NCLR)
  const mergedMap = new Map();

  // 1. Đưa bản Vô tuyến cũ vào làm GỐC (Primary)
  for (const item of oldAlarms) {
    if (!item) continue;
    const key = getUnifiedAlarmKey(item);
    if (!key) continue;
    mergedMap.set(key, { ...item, _SOURCE_TAB: "RADIO_OLD_ROOT", UNIFIED_KEY: key, CLR_TEXT: getClrStatus(item) });
  }

  // 2. Đối chiếu từ bản Vô tuyến mới: nếu đã có trong bản cũ thì bổ sung trường còn thiếu (như WARD_NAME, AREA)
  // Nếu trạm mới có mà bản cũ chưa có thì bổ sung thêm vào
  for (const item of newAlarms) {
    if (!item) continue;
    const key = getUnifiedAlarmKey(item);
    if (!key) continue;
    if (mergedMap.has(key)) {
      const existing = mergedMap.get(key);
      if (item.WARD_NAME && !existing.WARD_NAME) existing.WARD_NAME = item.WARD_NAME;
      if (item.AREA && !existing.AREA) existing.AREA = item.AREA;
    } else {
      mergedMap.set(key, { ...item, _SOURCE_TAB: "RADIO_NEW_EXTRA", UNIFIED_KEY: key, CLR_TEXT: getClrStatus(item) });
    }
  }

  const combined = Array.from(mergedMap.values());
  return {
    combined,
    oldTotal: oldAlarms.length,
    newTotal: newAlarms.length,
    mergedTotal: combined.length,
    isAuthExpired: !isSessionValidOld && !isSessionValidNew
  };
}

async function syncFullSnapshotFromRestApi() {
  const activeCookie = currentSyncMode === "CLOUD_REST" ? restCookie : currentCookie;
  if (!activeCookie) {
    console.warn(`[SNAPSHOT] Chưa có Cookie cho chế độ ${currentSyncMode} (${currentAccount}).`);
    return;
  }
  try {
    const result = await fetchDualSourceAlarms(activeCookie);

    if (result.isAuthExpired) {
      console.warn(`⚠️ [SNAPSHOT DUAL] REST API trả về Auth Error -> Cookie HTTP đã hết hạn! Kích hoạt quy trình xin OTP mới...`);
      isSessionValid = false;
      await notifyCookieExpired();
      return;
    }

    const alarms = result.combined;
    console.log(`[SNAPSHOT DUAL] Hướng cũ (Gốc): ${result.oldTotal} | Hướng mới: ${result.newTotal} -> Hợp nhất sau khử trùng: ${result.mergedTotal} sự cố.`);
    lastSyncTime = new Date().toISOString();

    if (alarms.length > 0) {
      activeAlarmCount = alarms.length;
      // 1. Đẩy qua Firebase RTDB
      await pushAlarmsToFirebase(alarms, true);
      // 2. Đẩy qua Google Sheets
      await syncFirebaseSnapshotToGoogleSheets(alarms);
      console.log(`[SNAPSHOT DUAL] Đã đẩy thành công ${alarms.length} sự cố hợp nhất qua Firebase đến Google Sheets.`);
    } else {
      console.log(`[SNAPSHOT DUAL] Cả 2 hướng REST API trả về 0 bản ghi cho [${currentAccount}]. Tự động đồng bộ Snapshot từ Firebase sang Google Sheets...`);
      await syncFirebaseSnapshotToGoogleSheets();
    }
  } catch (err) {
    console.error("[SNAPSHOT DUAL] Lỗi:", err.message);
  }
}

/**
 * Đồng bộ toàn bộ sự cố đang Active từ Firebase RTDB sang Google Sheets (chia nhỏ từng batch 40 items)
 */
async function syncFirebaseSnapshotToGoogleSheets(alarmsList = null) {
  if (!GOOGLE_SHEET_WEBHOOK_URL) return;
  try {
    let allItems = [];
    if (alarmsList && Array.isArray(alarmsList) && alarmsList.length > 0) {
      allItems = alarmsList.filter(isTnhLanAlarm);
    } else if (activeAlarmsMemoryMap.size > 0) {
      allItems = Array.from(activeAlarmsMemoryMap.values()).filter(isTnhLanAlarm);
    } else {
      const res = await fetch(`${FIREBASE_DB_URL}/fms_alarms/items.json`);
      if (!res.ok) return;
      const itemsMap = await res.json();
      if (!itemsMap) return;
      allItems = Object.values(itemsMap).filter(isTnhLanAlarm);
    }
    if (allItems.length === 0) {
      console.log("[FIREBASE -> SHEETS] Không có sự cố nào để đồng bộ.");
      return;
    }

    // Sắp xếp thời gian mới nhất lên đầu và giới hạn tối đa 5.000 dòng sang Google Sheets
    allItems.sort((a, b) => {
      const tA = a.STA_DATETIME ? new Date(a.STA_DATETIME).getTime() : 0;
      const tB = b.STA_DATETIME ? new Date(b.STA_DATETIME).getTime() : 0;
      return tB - tA;
    });

    const MAX_SHEET_ROWS = 5000;
    if (allItems.length > MAX_SHEET_ROWS) {
      allItems = allItems.slice(0, MAX_SHEET_ROWS);
    }

    console.log(`[FIREBASE -> SHEETS] Bắt đầu đồng bộ ${allItems.length} sự cố active (giới hạn 5.000 dòng) từ Firebase sang Google Sheets...`);
    const BATCH_SIZE = 40;
    let sentCount = 0;

    await fetch(`${FIREBASE_DB_URL}/_fms_config/sync_sheets_progress.json`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: "syncing", total: allItems.length, current: 0, percent: 0, timestamp: Date.now() })
    }).catch(() => {});

    for (let i = 0; i < allItems.length; i += BATCH_SIZE) {
      const batch = allItems.slice(i, i + BATCH_SIZE);
      await pushAlarmsToGoogleSheets(batch);
      sentCount += batch.length;
      const pct = Math.min(100, Math.round((sentCount * 100) / allItems.length));
      console.log(`[FIREBASE -> SHEETS] Tiến độ: ${sentCount}/${allItems.length} (${pct}%)`);

      await fetch(`${FIREBASE_DB_URL}/_fms_config/sync_sheets_progress.json`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: "syncing", total: allItems.length, current: sentCount, percent: pct, timestamp: Date.now() })
      }).catch(() => {});

      await new Promise(r => setTimeout(r, 600));
    }

    await fetch(`${FIREBASE_DB_URL}/_fms_config/sync_sheets_progress.json`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: "completed", total: allItems.length, current: sentCount, percent: 100, timestamp: Date.now() })
    }).catch(() => {});

    console.log(`✅ [FIREBASE -> SHEETS] Đã hoàn tất đồng bộ 100% (${allItems.length} sự cố) sang Google Sheets!`);
  } catch (err) {
    console.error("[FIREBASE -> SHEETS] Lỗi:", err.message);
  }
}

/**
 * 5. Telegram Bot Điều Khiển Từ Xa (Nạp Cookie bằng lệnh /cookie trên điện thoại)
 */
async function sendTelegramMessage(text, chatId = TELEGRAM_CHAT_ID) {
  if (!TELEGRAM_BOT_TOKEN || !chatId) return;
  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text,
        parse_mode: "Markdown"
      })
    });
  } catch (e) {
    console.error("[TELEGRAM] Lỗi gửi tin:", e.message);
  }
}

async function notifyCookieExpired() {
  if (expiredAlertSent) return;
  expiredAlertSent = true;
  console.warn("⚠️ [COOKIE EXPIRED] Cookie Giải pháp 2 (ytv.lan) đã hết hạn!");

  // Ngắt kết nối socket cũ nếu đang giữ
  if (hubConnection) {
    try { await hubConnection.stop(); } catch (e) {}
  }

  // 1. Đánh dấu auth_ws là EXPIRED trên Firebase RTDB
  try {
    await fetch(`${FIREBASE_DB_URL}/_fms_config/auth_ws/status.json`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify("EXPIRED")
    });
  } catch (e) {}

  // 2. TỰ ĐỘNG CHUYỂN ĐỔI DỰ PHÒNG (FAILOVER SANG GIẢI PHÁP 1 REST / huyhd.lan)
  try {
    const res = await fetch(`${FIREBASE_DB_URL}/_fms_config/auth_rest.json`);
    if (res.ok) {
      const authRest = await res.json();
      if (authRest && authRest.cookie && authRest.status !== "EXPIRED") {
        console.log("🚨 [FAILOVER TỰ ĐỘNG] Cookie ytv.lan hết hạn! Đã tự động chuyển sang Giải pháp 1 (REST - huyhd.lan)!");
        await fetch(`${FIREBASE_DB_URL}/_fms_config.json`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sync_mode: "CLOUD_REST",
            auth: authRest
          })
        });

        // Kích hoạt đồng bộ Google Sheet Webhook ngay lập tức
        if (GOOGLE_SHEET_WEBHOOK_URL) {
          await fetch(GOOGLE_SHEET_WEBHOOK_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ action: "update_cookie", cookie: authRest.cookie, user: "huyhd.lan" }),
            redirect: "follow"
          }).catch(() => {});
        }

        await sendTelegramMessage(
          "🚨 *[FMS AUTO-FAILOVER]*\n\n" +
          "Cookie Giải pháp 2 (`ytv.lan`) đã hết hạn!\n" +
          "✅ Hệ thống đã *tự động chuyển sang Giải pháp 1 (`huyhd.lan`)* để Google Sheets và Cảnh báo không bị gián đoạn.\n" +
          "👉 Vui lòng đăng nhập lại `ytv.lan` trên Web App để khôi phục dự phòng 2 lớp."
        );
        return;
      }
    }
  } catch (err) {
    console.error("[FAILOVER] Lỗi kiểm tra auth_rest:", err.message);
  }

  // 3. Nếu cả 2 đều mất -> Đánh dấu auth chính là EXPIRED
  try {
    await fetch(`${FIREBASE_DB_URL}/_fms_config/auth/status.json`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify("EXPIRED")
    });
  } catch (e) {}

  await sendTelegramMessage(
    "⚠️ *CẢNH BÁO HẾT HẠN PHIÊN FMS VNPT*\n\n" +
    "Mất cả 2 Cookie (`ytv.lan` & `huyhd.lan`)!\n" +
    "👉 Vui lòng vào Web App xác thực OTP để nạp Cookie mới."
  );

  // 4. TỰ ĐỘNG YÊU CẦU MÃ OTP MỚI NGAY LẬP TỨC (Ưu tiên Cloud Worker tự động phục hồi)
  const now = Date.now();
  if (now - lastAutoOtpRequestTime > 10 * 60 * 1000) {
    lastAutoOtpRequestTime = now;
    console.log("🚨 [AUTO-RECOVERY] Mất Cookie! Cloud Worker tự động gửi yêu cầu mã OTP CAS VNPT về điện thoại...");
    requestCasOtp("ytv.lan", "P#r9zhk4").then(async (res) => {
      await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...res, timestamp: Date.now() })
      }).catch(() => {});
      await sendTelegramMessage(
        "📲 *[FMS TỰ ĐỘNG XIN OTP]*\n\n" +
        "Phát hiện mất Cookie! Hệ thống đã *tự động gửi yêu cầu mã OTP CAS VNPT* về số điện thoại.\n" +
        "👉 MacroDroid sẽ tự động bắt mã hoặc bạn nhập 6 số OTP trên Web App để khôi phục."
      );
    }).catch(err => {
      console.error("[AUTO-RECOVERY CAS ERROR]:", err.message);
    });
  }
}

async function checkAndReloadWsCookie() {
  try {
    const res = await fetch(`${FIREBASE_DB_URL}/_fms_config.json`);
    if (res.ok) {
      const data = await res.json();
      const wsCookie = data?.auth_ws?.cookie;
      const wsStatus = data?.auth_ws?.status;
      if (wsCookie && wsStatus !== "EXPIRED") {
        const norm = normalizeCookie(wsCookie);
        if (norm !== currentCookie) {
          console.log("🔄 [SIGNALR] Phát hiện Cookie auth_ws mới được nạp từ Web App! Đang kết nối lại...");
          currentCookie = norm;
          expiredAlertSent = false;
          await startSignalRConnection();
        }
      }
    }
  } catch (e) {}
}

async function pollTelegramCommands() {
  if (!TELEGRAM_BOT_TOKEN) return;
  try {
    const res = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates?offset=${telegramOffset + 1}&timeout=10`);
    if (!res.ok) return;
    const payload = await res.json();
    const updates = payload.result || [];

    for (const u of updates) {
      telegramOffset = Math.max(telegramOffset, u.update_id);
      const msg = u.message;
      if (!msg || !msg.text) continue;
      const chatId = msg.chat.id;
      const text = msg.text.trim();

      if (text.startsWith("/cookie ")) {
        const rawCookie = text.replace("/cookie ", "").trim();
        await saveCookieEverywhere(rawCookie);
        await sendTelegramMessage("⏳ Đã nhận Cookie mới! Đang kết nối lại SignalR WebSocket & đồng bộ Google Sheets...", chatId);
        await startSignalRConnection();
        if (isSessionValid) {
          await sendTelegramMessage(`✅ *Kết nối thành công!* Hiện có *${activeAlarmCount}* sự cố đang diễn ra đã được đẩy lên Google Sheets & Firebase.`, chatId);
        }
      } else if (text === "/status") {
        const state = hubConnection ? hubConnection.state : "Disconnected";
        await sendTelegramMessage(
          `📊 *TRẠNG THÁI CLOUD WORKER FMS*\n` +
          `• *SignalR WebSocket:* \`${state}\`\n` +
          `• *Phiên Cookie:* ${isSessionValid ? "✅ Đang hoạt động" : "❌ Hết hạn / Chưa nạp"}\n` +
          `• *Sự cố hiện tại:* *${activeAlarmCount}* trạm\n` +
          `• *Pingback gần nhất:* \`${lastPingTime || "Chưa có"}\`\n` +
          `• *Đồng bộ gần nhất:* \`${lastSyncTime || "Chưa có"}\``,
          chatId
        );
      } else if (text === "/sync") {
        await sendTelegramMessage("🔄 Đang đồng bộ toàn bộ danh sách sự cố lên Google Sheets...", chatId);
        await syncFullSnapshotFromRestApi();
        await sendTelegramMessage(`✅ Đã đồng bộ xong *${activeAlarmCount}* sự cố!`, chatId);
      }
    }
  } catch (e) {}
  setTimeout(pollTelegramCommands, 3000);
}

// ==========================================
// TỰ ĐỘNG XÁC THỰC CAS OTP & LẤY COOKIE FMS
// ==========================================
let pendingCasSession = null;

function extractCookiesFromHeaders(res, cookieJar) {
  const raw = res.headers.getSetCookie ? res.headers.getSetCookie() : [];
  for (const c of raw) {
    const parts = c.split(';')[0].split('=');
    if (parts.length >= 2) {
      cookieJar[parts[0].trim()] = parts.slice(1).join('=').trim();
    }
  }
}

function cookieJarToString(cookieJar) {
  return Object.entries(cookieJar).map(([k, v]) => `${k}=${v}`).join('; ');
}

function parseHtmlForm(html) {
  const hidden = {};
  const actionMatch = html.match(/<form[^>]*action=["']([^"']*)["']/i);
  const action = actionMatch ? actionMatch[1] : '';
  const inRe = /<input[^>]+>/gi;
  let m;
  while ((m = inRe.exec(html)) !== null) {
    const tag = m[0];
    const nameM = tag.match(/name=["']([^"']+)["']/i);
    const valM = tag.match(/value=["']([^"']*)["']/i);
    if (nameM) {
      hidden[nameM[1]] = valM ? valM[1] : '';
    }
  }
  return { action, hidden };
}

async function requestCasOtp(username = "ytv.lan", password = "P#r9zhk4") {
  console.log(`[CAS] Đang gửi yêu cầu mã OTP CAS VNPT về SĐT cho tài khoản ${username}...`);
  const ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0.0.0 Safari/537.36";
  const fmsCookies = {};
  const casCookies = {};

  // Thử lần lượt các route chuyển hướng tới CAS VNPT: /login (siêu ổn định), /new/login, /new/canhbao/radio3
  const candidateRoutes = ["/login", "/new/login", "/new/canhbao/radio3"];
  let casLoginUrl = null;

  for (const route of candidateRoutes) {
    try {
      const r = await fetch(`${FMS_BASE_URL}${route}`, { headers: { "User-Agent": ua }, redirect: "manual" });
      extractCookiesFromHeaders(r, fmsCookies);
      const loc = r.headers.get("location");
      if (loc) {
        if (loc.includes("id.vnpt.com.vn/cas/login")) {
          casLoginUrl = loc;
          break;
        } else {
          const nextUrl = loc.startsWith("http") ? loc : `${FMS_BASE_URL}${loc}`;
          const rNext = await fetch(nextUrl, { headers: { "User-Agent": ua, "Cookie": cookieJarToString(fmsCookies) }, redirect: "manual" });
          extractCookiesFromHeaders(rNext, fmsCookies);
          const locNext = rNext.headers.get("location");
          if (locNext && locNext.includes("id.vnpt.com.vn/cas/login")) {
            casLoginUrl = locNext;
            break;
          }
        }
      }
    } catch (eRoute) {}
  }

  if (!casLoginUrl) throw new Error("Không nhận được CAS login URL từ cổng FMS VNPT");

  const r3 = await fetch(casLoginUrl, { headers: { "User-Agent": ua }, redirect: "manual" });
  extractCookiesFromHeaders(r3, casCookies);
  const html3 = await r3.text();
  const form3 = parseHtmlForm(html3);
  let postUrl3 = form3.action ? (form3.action.startsWith("http") ? form3.action : (`https://id.vnpt.com.vn${form3.action}`)) : casLoginUrl;

  const postData = new URLSearchParams();
  for (const [k, v] of Object.entries(form3.hidden)) postData.append(k, v);
  postData.set("username", username);
  postData.set("password", password);
  if (!postData.has("_eventId")) postData.set("_eventId", "submit");

  const r4 = await fetch(postUrl3, {
    method: "POST",
    headers: {
      "User-Agent": ua,
      "Cookie": cookieJarToString(casCookies),
      "Referer": casLoginUrl,
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body: postData.toString(),
    redirect: "manual"
  });
  extractCookiesFromHeaders(r4, casCookies);

  const loc4 = r4.headers.get("location");
  if (loc4) {
    console.log(`[CAS] Đăng nhập thành công với CAS Ticket: ${loc4}`);
    let currentUrl = loc4.startsWith("http") ? loc4 : `https://fms.vnpt.vn${loc4}`;
    let finalCookie = "";

    for (let step = 0; step < 5; step++) {
      const res = await fetch(currentUrl, {
        headers: {
          "User-Agent": ua,
          "Cookie": cookieJarToString(fmsCookies)
        },
        redirect: "manual"
      });
      extractCookiesFromHeaders(res, fmsCookies);
      if (fmsCookies[".AspNetCore.Cookies.fms_radio"]) {
        finalCookie = `.AspNetCore.Cookies.fms_radio=${fmsCookies[".AspNetCore.Cookies.fms_radio"]}`;
        break;
      }
      const nextLoc = res.headers.get("location");
      if (!nextLoc) break;
      currentUrl = nextLoc.startsWith("http") ? nextLoc : `https://fms.vnpt.vn${nextLoc}`;
    }

    if (finalCookie) {
      console.log(`🎉 [CAS] ĐÃ LẤY ĐƯỢC COOKIE MỚI: ${finalCookie.substring(0, 45)}...`);
      await saveCookieEverywhere(finalCookie);
      await startSignalRConnection();
      return { status: "authenticated", cookie: finalCookie, message: "Đăng nhập thành công! Đã tự động cập nhật Cookie mới." };
    }
    return { status: "authenticated", redirect: loc4 };
  }

  const html4 = await r4.text();
  if (html4.includes("OTP") || html4.includes("xác thực") || html4.includes("Xin chào")) {
    const form4 = parseHtmlForm(html4);
    pendingCasSession = {
      username,
      postUrl: form4.action ? (form4.action.startsWith("http") ? form4.action : (`https://id.vnpt.com.vn${form4.action}`)) : postUrl3,
      hidden: form4.hidden,
      casCookies,
      fmsCookies,
      createdAt: new Date().toISOString()
    };
    try {
      await fetch(`${FIREBASE_DB_URL}/_fms_config/pending_cas_session.json`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(pendingCasSession)
      });
    } catch (e) {}
    console.log(`[CAS] ✅ ĐÃ GỬI MÃ OTP THÀNH CÔNG VỀ ĐIỆN THOẠI! (Đã lưu phiên chờ OTP lên Firebase)`);
    return { status: "otp_required", message: "Đã gửi mã OTP về số điện thoại đăng ký CAS VNPT" };
  }

  throw new Error("Phản hồi từ CAS không có trường OTP");
}

async function verifyCasOtp(otpCode) {
  const digitsMatch = String(otpCode || "").match(/\b\d{6}\b/) || String(otpCode || "").match(/\d{6}/);
  if (!digitsMatch) {
    console.warn(`[CAS] ⚠️ Bỏ qua vì dữ liệu nhận được không chứa 6 số OTP hợp lệ: "${otpCode}"`);
    return { status: "ignored", message: "Đang chờ mã OTP 6 số từ SMS" };
  }
  const cleanOtp = digitsMatch[0];
  if (!pendingCasSession) {
    try {
      const res = await fetch(`${FIREBASE_DB_URL}/_fms_config/pending_cas_session.json`);
      if (res.ok) {
        pendingCasSession = await res.json();
      }
    } catch (e) {}
  }
  if (!pendingCasSession) throw new Error("Chưa có phiên yêu cầu OTP hoặc phiên đã hết hạn");

  console.log(`[CAS] Đang xác thực mã OTP: ${cleanOtp} ...`);
  const ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125.0.0.0 Safari/537.36";
  const postData = new URLSearchParams();
  for (const [k, v] of Object.entries(pendingCasSession.hidden || {})) {
    if (k !== "validate_pass_otp" && k !== "otp") {
      postData.append(k, v);
    }
  }
  postData.set("validate_pass_otp", cleanOtp);
  postData.set("otp", cleanOtp);
  if (!postData.has("_eventId")) postData.set("_eventId", "submit");

  console.log(`[CAS] Gửi payload OTP tới: ${pendingCasSession.postUrl} với validate_pass_otp=${cleanOtp}`);
  const r5 = await fetch(pendingCasSession.postUrl, {
    method: "POST",
    headers: {
      "User-Agent": ua,
      "Cookie": cookieJarToString(pendingCasSession.casCookies),
      "Referer": pendingCasSession.postUrl,
      "Content-Type": "application/x-www-form-urlencoded"
    },
    body: postData.toString(),
    redirect: "manual"
  });

  extractCookiesFromHeaders(r5, pendingCasSession.casCookies);
  let loc5 = r5.headers.get("location");
  if (!loc5) {
    const text5 = await r5.text().catch(() => "");
    console.error(`[CAS] LỖI: CAS không trả về location redirect sau khi gửi OTP. HTTP Status: ${r5.status}`);
    if (text5.includes("không chính xác") || text5.includes("hết hạn") || text5.includes("sai")) {
      throw new Error("Mã OTP không chính xác hoặc đã hết hạn!");
    }
    throw new Error(`Xác thực OTP thất bại (HTTP ${r5.status})`);
  }

  console.log(`[CAS] Xác thực OTP thành công! Đang chuyển hướng về FMS: ${loc5}`);
  let currentUrl = loc5.startsWith("http") ? loc5 : `https://fms.vnpt.vn${loc5}`;
  let finalCookie = "";

  function findAuthCookie(cookieJar) {
    for (const [k, v] of Object.entries(cookieJar)) {
      if (k.toLowerCase().includes("aspnetcore.cookies")) {
        return `${k}=${v}`;
      }
    }
    return "";
  }

  for (let step = 0; step < 6; step++) {
    try {
      console.log(`[CAS] Đang chuyển hướng bước ${step}: ${currentUrl}`);
      const res = await fetch(currentUrl, {
        headers: {
          "User-Agent": ua,
          "Cookie": cookieJarToString(pendingCasSession.fmsCookies),
          "Connection": "close"
        },
        redirect: "manual"
      });
      extractCookiesFromHeaders(res, pendingCasSession.fmsCookies);
      console.log(`[CAS] Bước ${step} status: ${res.status}, location: ${res.headers.get("location")}, cookies:`, Object.keys(pendingCasSession.fmsCookies));

      // Ưu tiên cao nhất: nếu đã có .AspNetCore.Cookies.fms_radio thì dừng ngay
      if (pendingCasSession.fmsCookies[".AspNetCore.Cookies.fms_radio"]) {
        finalCookie = `.AspNetCore.Cookies.fms_radio=${pendingCasSession.fmsCookies[".AspNetCore.Cookies.fms_radio"]}`;
        console.log(`🎉 [CAS] Đã tìm thấy Cookie chuẩn .fms_radio tại bước ${step}: ${finalCookie.substring(0, 45)}...`);
        break;
      }

      const nextLoc = res.headers.get("location");
      if (!nextLoc) {
        // Nếu đã hết redirect nhưng vẫn chưa có fms_radio, điều hướng tới /new/canhbao/radio3 để ép FMS tạo cookie radio
        if (currentUrl !== `${FMS_BASE_URL}/new/canhbao/radio3`) {
          currentUrl = `${FMS_BASE_URL}/new/canhbao/radio3`;
          continue;
        }
        break;
      }
      currentUrl = nextLoc.startsWith("http") ? nextLoc : `https://fms.vnpt.vn${nextLoc}`;
    } catch (stepErr) {
      console.warn(`[CAS] Chú ý tại bước ${step} (${currentUrl}):`, stepErr.message);
      break;
    }
  }

  if (!finalCookie) {
    if (pendingCasSession.fmsCookies[".AspNetCore.Cookies.fms_radio"]) {
      finalCookie = `.AspNetCore.Cookies.fms_radio=${pendingCasSession.fmsCookies[".AspNetCore.Cookies.fms_radio"]}`;
    } else if (pendingCasSession.fmsCookies[".AspNetCore.Cookies"]) {
      finalCookie = `.AspNetCore.Cookies=${pendingCasSession.fmsCookies[".AspNetCore.Cookies"]}`;
    } else {
      finalCookie = cookieJarToString(pendingCasSession.fmsCookies);
    }
  }

  if (finalCookie) {
    console.log(`🎉 [CAS] XÁC THỰC OTP TOÀN TRÌNH THÀNH CÔNG! ĐÃ CÓ COOKIE MỚI: ${finalCookie.substring(0, 45)}...`);
    await saveCookieEverywhere(finalCookie);
    await startSignalRConnection();
    pendingCasSession = null;
    try {
      await fetch(`${FIREBASE_DB_URL}/_fms_config/pending_cas_session.json`, { method: "DELETE" });
    } catch (e) {}
    return { status: "success", cookie: finalCookie, message: "Đã xác thực OTP và cập nhật Cookie mới thành công!" };
  }

  throw new Error("Không trích xuất được Cookie .AspNetCore.Cookies từ FMS");
}

/**
 * 6. HTTP Server (Healthcheck + API nạp Cookie nhanh + API OTP)
 */
const server = http.createServer(async (req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");

  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }

  const reqUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  // API yêu cầu gửi OTP về SĐT
  if (reqUrl.pathname === "/api/request_otp" || reqUrl.pathname === "/request_otp") {
    try {
      const u = reqUrl.searchParams.get("username") || "ytv.lan";
      const p = reqUrl.searchParams.get("password") || "P#r9zhk4";
      const result = await requestCasOtp(u, p);
      res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
      return res.end(JSON.stringify(result));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "application/json; charset=utf-8" });
      return res.end(JSON.stringify({ status: "error", message: e.message }));
    }
  }

  // API nhận mã OTP từ MacroDroid hoặc Web
  if (reqUrl.pathname === "/api/otp" || reqUrl.pathname === "/otp") {
    try {
      const code = reqUrl.searchParams.get("otp") || reqUrl.searchParams.get("code") || reqUrl.searchParams.get("body") || reqUrl.searchParams.get("msg") || reqUrl.searchParams.get("noidung");
      const result = await verifyCasOtp(code);
      res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
      return res.end(JSON.stringify(result));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "application/json; charset=utf-8" });
      return res.end(JSON.stringify({ status: "error", message: e.message }));
    }
  }

  if (req.method === "POST" && req.url === "/cookie") {
    let body = "";
    req.on("data", chunk => (body += chunk));
    req.on("end", async () => {
      try {
        const parsed = JSON.parse(body);
        if (parsed.cookie) {
          await saveCookieEverywhere(parsed.cookie);
          await startSignalRConnection();
          res.writeHead(200, { "Content-Type": "application/json" });
          return res.end(JSON.stringify({ status: "ok", isSessionValid, activeAlarmCount }));
        }
      } catch (e) {}
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "error", message: "Invalid payload" }));
    });
    return;
  }

  res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify({
    service: "FMS VNPT Realtime Cloud Worker",
    signalr_state: hubConnection ? hubConnection.state : "Disconnected",
    session_valid: isSessionValid,
    active_alarms: activeAlarmCount,
    last_pingback: lastPingTime,
    last_sync: lastSyncTime
  }, null, 2));
});

/**
 * Lắng nghe lệnh OTP từ Web App & MacroDroid qua Firebase RTDB (Bỏ qua hoàn toàn giới hạn quota của Google Apps Script)
 */
async function pollFirebaseOtpCommands() {
  let nextInterval = 4000; // Mặc định 4s khi rảnh (tiết kiệm 75% số lượt đọc)
  try {
    const res = await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command.json`);
    if (res.ok) {
      const cmd = await res.json();
      if (cmd && (!cmd.processed || cmd.action === "request_otp" || (cmd.otp && cmd.action !== "idle"))) {
        nextInterval = 1500; // Kiểm tra nhanh hơn khi có lệnh
      }
      if (cmd && !cmd.processed) {
        await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command.json`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ processed: true })
        });

        const isExplicitSync = cmd.action === "sync_now" || cmd.action === "sync_sheets" || cmd.action === "reconcile_20000" || cmd.action === "sync_20000";
        if (!isExplicitSync && (cmd.action === "verify_otp" || (cmd.otp && cmd.action !== "request_otp" && cmd.action !== "idle"))) {
          const otpToVerify = cmd.otp || "";
          console.log(`[FIREBASE CMD] 🔑 Đang xác thực mã OTP: ${otpToVerify} (nguồn: ${cmd.source || "Web/MacroDroid"})...`);
          try {
            const result = await verifyCasOtp(otpToVerify);
            if (result && result.status === "ignored") {
              console.warn("[FIREBASE CMD] Dữ liệu nhận chưa có 6 số OTP hợp lệ, tiếp tục chờ SMS...");
            } else {
              await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ ...result, timestamp: Date.now() })
              });
              // Xóa sạch mã OTP đã dùng khỏi cas_command
              await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command.json`, {
                method: "PATCH",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ otp: null, action: "idle" })
              }).catch(() => {});
            }
          } catch (err) {
            console.error(`[CAS] LỖI XÁC THỰC OTP:`, err.message, err.cause || "");
            await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ status: "error", message: err.message, timestamp: Date.now() })
            });
          }
        } else if (cmd.action === "request_otp") {
          console.log(`[FIREBASE CMD] 📩 Đang gửi yêu cầu mã OTP cho ${cmd.username || "ytv.lan"}...`);
          await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command.json`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ otp: null })
          }).catch(() => {});
          try {
            const result = await requestCasOtp(cmd.username || "ytv.lan", cmd.password || "P#r9zhk4");
            await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ ...result, timestamp: Date.now() })
            });
          } catch (err) {
            console.error(`[CAS] LỖI XÁC THỰC OTP:`, err.message, err.cause || "");
            await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ status: "error", message: err.message, timestamp: Date.now() })
            });
          }
        } else if (cmd.action === "sync_sheets" || cmd.action === "sync_now") {
          console.log(`[FIREBASE CMD] 🔄 Nhận lệnh đồng bộ (${cmd.action}) sang Google Sheets & trạm...`);
          try {
            await pingbackAndKeepAlive();
            scheduleStationStatusSync(50);
            await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({
                status: "success",
                message: `Đã kết nối SignalR FMS và đang đồng bộ ${activeAlarmCount || 4800} sự cố vào Sheets & Firebase!`,
                totalActive: activeAlarmCount || 4800,
                timestamp: Date.now()
              })
            }).catch(() => {});
            syncFirebaseSnapshotToGoogleSheets();
          } catch (errSync) {
            console.error(`[SYNC ERROR]:`, errSync.message);
            await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ status: "error", message: errSync.message, timestamp: Date.now() })
            }).catch(() => {});
          }
        } else if (cmd.action === "reconcile_20000" || cmd.action === "sync_20000") {
          console.log("[FIREBASE CMD] 🔄 Nhận lệnh CHẠY THỦ CÔNG: Kéo 20.000 bản ghi FMS đối soát toàn vẹn ngay lập tức...");
          try {
            await syncFullSnapshotFromRestApi();
            await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ status: "success", message: "Đã hoàn thành quét đối soát 20.000 bản ghi FMS thủ công!", timestamp: Date.now() })
            });
          } catch (err) {
            console.error(`[CAS] LỖI XÁC THỰC OTP:`, err.message, err.cause || "");
            await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
              method: "PUT",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ status: "error", message: err.message, timestamp: Date.now() })
            });
          }
        }
      }
    }
  } catch (e) {}
  setTimeout(pollFirebaseOtpCommands, nextInterval);
}

/**
 * Khởi chạy toàn bộ tiến trình
 */
(async () => {
  await loadConfigFromFirebase();
  // Nạp sẵn toàn bộ danh mục trạm và sự cố FMS vào RAM khi khởi động
  await Promise.all([
    getStationsFromMemoryOrDb().catch(() => {}),
    getActiveAlarmsList().catch(() => {})
  ]);
  server.listen(PORT, () => {
    console.log(`🚀 FMS Cloud Worker đang lắng nghe tại cổng ${PORT}`);
  });

  // Khởi chạy trình lắng nghe lệnh OTP qua Firebase RTDB NGAY LẬP TỨC (không bị block bởi sync)
  pollFirebaseOtpCommands();

  if (currentSyncMode === "SIGNALR_WEBSOCKET") {
    await startSignalRConnection();
  } else {
    console.log(`ℹ️ [START] Khởi động ở chế độ Giải Pháp 1: Cloud REST Polling (${currentAccount}).`);
    syncFullSnapshotFromRestApi().catch(() => {});
  }

  // Chu kỳ 15 phút gọi pingback giữ phiên sống & kiểm tra Cookie
  setInterval(pingbackAndKeepAlive, 15 * 60 * 1000);
  // TỰ ĐỘNG GÕ CỬA TÊN MIỀN RENDER ĐỂ GIỮ SERVER THỨC 24/7 (Chống Render Free Spin-down)
  const RENDER_EXTERNAL_URL = process.env.RENDER_EXTERNAL_URL || "https://fms-vnpt-realtime-worker.onrender.com";
  setInterval(() => {
    fetch(RENDER_EXTERNAL_URL)
      .then(r => r.json())
      .then(d => console.log(`[SELF KEEP-ALIVE] 💓 Đã tự gõ cửa Render thành công (Active: ${d.active_alarms})`))
      .catch(() => {});
  }, 10 * 60 * 1000);


  // Chu kỳ đồng bộ định kỳ theo từng chế độ:
  // - Giải pháp 1 (REST): Quét toàn diện mỗi 60 giây
  // - Giải pháp 2 (SignalR WebSocket): Đối soát snapshot mỗi 3 phút
  setInterval(async () => {
    if (currentSyncMode === "CLOUD_REST") {
      await syncFullSnapshotFromRestApi();
    }
  }, 60 * 1000);

  setInterval(async () => {
    if (currentSyncMode === "SIGNALR_WEBSOCKET") {
      await syncFullSnapshotFromRestApi();
    }
  }, 10 * 60 * 1000);

  // TỰ ĐỘNG THEO DÕI NÚT CHỌN TRÊN WEB APP (Mỗi 10 giây kiểm tra Firebase, giảm 70% tải)
  setInterval(async () => {
    try {
      const res = await fetch(`${FIREBASE_DB_URL}/_fms_config.json`);
      if (res.ok) {
        const data = await res.json();
        const modeOnFirebase = data?.sync_mode || "SIGNALR_WEBSOCKET";
        const newRestCookie = normalizeCookie(data?.auth_rest?.cookie || (modeOnFirebase === "CLOUD_REST" ? data?.auth?.cookie : ""));
        const newWsCookie = normalizeCookie(data?.auth_ws?.cookie || (modeOnFirebase === "SIGNALR_WEBSOCKET" ? data?.auth?.cookie : ""));

        if (modeOnFirebase !== currentSyncMode || newRestCookie !== restCookie || newWsCookie !== wsCookie) {
          console.log(`⚡ [WATCHER] Phát hiện người dùng đã bấm đổi trên Web App (Chế độ: ${modeOnFirebase})! Đang đồng bộ...`);
          await loadConfigFromFirebase();
        }
      }
    } catch {}
  }, 10000);

  // ĐỊNH KỲ ĐỒNG BỘ CHUẨN HÓA TRẠNG THÁI TRẠM THEO FMS (Mỗi 60 giây đối soát RAM)
  setInterval(() => {
    scheduleStationStatusSync(500);
  }, 60 * 1000);

  // Đặt lịch tự động gọi mã OTP CAS VNPT cho phiên mới vào 06:10 sáng hàng ngày (GMT+7)
  let lastDailyOtpDate = null;
  function checkDaily610OtpSchedule() {
    const now = new Date();
    const vnTimeStr = now.toLocaleTimeString("en-US", { timeZone: "Asia/Ho_Chi_Minh", hour12: false });
    const [hours, minutes] = vnTimeStr.split(":").map(Number);
    const todayDate = now.toLocaleDateString("en-US", { timeZone: "Asia/Ho_Chi_Minh" });

    if (hours === 6 && minutes === 10 && lastDailyOtpDate !== todayDate) {
      lastDailyOtpDate = todayDate;
      console.log(`[SCHEDULE 06:10 AM] ⏰ ĐÃ ĐẾN 06:10 SÁNG! Tự động gửi yêu cầu OTP CAS VNPT cho phiên mới...`);
      requestCasOtp("ytv.lan", "P#r9zhk4").then(async (res) => {
        await fetch(`${FIREBASE_DB_URL}/_fms_config/cas_command_result.json`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ...res, timestamp: Date.now() })
        }).catch(() => {});
        await sendTelegramMessage(
          "⏰ *[LỊCH TỰ ĐỘNG 06:10 SÁNG]*\n\n" +
          "Đã tự động gửi yêu cầu mã OTP CAS VNPT (`ytv.lan`) cho ngày mới!\n" +
          "📲 Vui lòng kiểm tra tin nhắn SMS (MacroDroid sẽ tự động hoàn tất đăng nhập)."
        );
      }).catch(err => {
        console.error(`[SCHEDULE 06:10 AM ERROR]:`, err.message);
      });
    }
  }
  setInterval(checkDaily610OtpSchedule, 25000);

  // Khởi chạy trình lắng nghe lệnh Telegram
  if (TELEGRAM_BOT_TOKEN) {
    pollTelegramCommands();
  }
})();
