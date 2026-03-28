import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.join(__dirname, "data");
const SQLITE_PATH = path.join(DATA_DIR, "app.db");
const ENV_PATH = path.join(__dirname, ".env");
const DEFAULT_APP_DB = {
  tickets: [],
  cooldowns: {},
  vouchers: [],
  voucherLinks: {},
  voucherCooldowns: {},
  liveVisitors: {},
  paymentVisits: [],
  paymentVisitCooldowns: {}
};
const DEFAULT_VOUCHER_LINKS = {
  "150": [
    { label: "Voucher Hub 150", url: "https://www.g2a.com/crypto-voucher-150-usd-key-global-i10000337580022?suid=e6fe5022-80bd-40d8-b984-968ba2392740" },
    { label: "Voucher Shop 150", url: "https://example.com/voucher/150/shop" },
    { label: "Voucher Alt 150", url: "https://example.com/voucher/150/alt" }
  ],
  "200": [
    { label: "Purchase Voucher: G2A.com", url: "https://www.g2a.com/crypto-voucher-200-usd-key-global-i10000337580029?suid=b7c37afc-b02f-4e1f-a478-c9ed0a556dc0" },
    { label: "Voucher Shop 200", url: "https://example.com/voucher/200/shop" },
    { label: "Voucher Alt 200", url: "https://example.com/voucher/200/alt" }
  ],
  "250": [
    { label: "Voucher Hub 250", url: "https://example.com/voucher/250/main" },
    { label: "Voucher Shop 250", url: "https://example.com/voucher/250/shop" },
    { label: "Voucher Alt 250", url: "https://example.com/voucher/250/alt" }
  ],
  "300": [
    { label: "Voucher Hub 300", url: "https://example.com/voucher/300/main" },
    { label: "Voucher Shop 300", url: "https://example.com/voucher/300/shop" },
    { label: "Voucher Alt 300", url: "https://example.com/voucher/300/alt" }
  ],
  "350": [
    { label: "Voucher Hub 350", url: "https://example.com/voucher/350/main" },
    { label: "Voucher Shop 350", url: "https://example.com/voucher/350/shop" },
    { label: "Voucher Alt 350", url: "https://example.com/voucher/350/alt" }
  ],
  "450": [
    { label: "Voucher Hub 450", url: "https://example.com/voucher/450/main" },
    { label: "Voucher Shop 450", url: "https://example.com/voucher/450/shop" },
    { label: "Voucher Alt 450", url: "https://example.com/voucher/450/alt" }
  ],
  "500": [
    { label: "Voucher Hub 500", url: "https://example.com/voucher/500/main" },
    { label: "Voucher Shop 500", url: "https://example.com/voucher/500/shop" },
    { label: "Voucher Alt 500", url: "https://example.com/voucher/500/alt" }
  ],
  "800": [
    { label: "Voucher Hub 800", url: "https://example.com/voucher/800/main" },
    { label: "Voucher Shop 800", url: "https://example.com/voucher/800/shop" },
    { label: "Voucher Alt 800", url: "https://example.com/voucher/800/alt" }
  ]
};
const DEFAULT_BOT_STATE = {
  seen_voucher_ids: [],
  seen_payment_visit_ids: [],
  muted_ticket_ids: [],
  pending_reply_ticket_id: ""
};

function loadEnvFile() {
  if (!fs.existsSync(ENV_PATH)) {
    return;
  }

  const lines = fs.readFileSync(ENV_PATH, "utf8").split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }

    const separatorIndex = line.indexOf("=");
    if (separatorIndex === -1) {
      continue;
    }

    const key = line.slice(0, separatorIndex).trim();
    const value = line.slice(separatorIndex + 1).trim();
    if (key && !process.env[key]) {
      process.env[key] = value;
    }
  }
}

loadEnvFile();

const PORT = Number(process.env.PORT || 8080);
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "";
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || "";
const ADMIN_RESOLVE_TOKEN = process.env.ADMIN_RESOLVE_TOKEN || "";
const HOODPAY_CREATE_PAYMENT_URL = process.env.HOODPAY_CREATE_PAYMENT_URL || "";
const HOODPAY_API_KEY = process.env.HOODPAY_API_KEY || "";
const HOODPAY_BUSINESS_ID = process.env.HOODPAY_BUSINESS_ID || "";
const HOODPAY_RETURN_URL = process.env.HOODPAY_RETURN_URL || "";
const HOODPAY_NOTIFY_URL = process.env.HOODPAY_NOTIFY_URL || "";

if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}

const sqlite = new Database(SQLITE_PATH);
sqlite.pragma("journal_mode = WAL");
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS kv_store (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  )
`);
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS voucher_links (
    price_key TEXT NOT NULL,
    sort_order INTEGER NOT NULL,
    label TEXT NOT NULL,
    url TEXT NOT NULL,
    PRIMARY KEY (price_key, sort_order)
  )
`);
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS tickets (
    id TEXT PRIMARY KEY,
    name TEXT,
    topic TEXT,
    order_id TEXT,
    status TEXT,
    person_key TEXT,
    ip TEXT,
    user_agent TEXT,
    fingerprint TEXT,
    browser_name TEXT,
    platform TEXT,
    created_at TEXT,
    updated_at TEXT,
    resolved_at TEXT
  )
`);
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS ticket_messages (
    id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    sender TEXT,
    body TEXT,
    created_at TEXT,
    sort_order INTEGER NOT NULL DEFAULT 0
  )
`);
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS vouchers (
    id TEXT PRIMARY KEY,
    created_at TEXT,
    person_key TEXT,
    ip TEXT,
    user_agent TEXT,
    product_id TEXT,
    product_name TEXT,
    option_id TEXT,
    option_label TEXT,
    price TEXT,
    customer_name TEXT,
    customer_email TEXT,
    customer_notes TEXT,
    voucher_code TEXT,
    fingerprint TEXT,
    browser_name TEXT,
    platform TEXT
  )
`);
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS live_visitors (
    person_key TEXT PRIMARY KEY,
    ip TEXT,
    browser_name TEXT,
    platform TEXT,
    page TEXT,
    path_name TEXT,
    title TEXT,
    last_seen_at INTEGER
  )
`);
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS payment_visits (
    id TEXT PRIMARY KEY,
    created_at TEXT,
    person_key TEXT,
    ip TEXT,
    user_agent TEXT,
    order_number TEXT,
    product_id TEXT,
    product_name TEXT,
    option_id TEXT,
    option_label TEXT,
    price TEXT,
    customer_name TEXT,
    customer_email TEXT,
    customer_notes TEXT,
    fingerprint TEXT,
    browser_name TEXT,
    platform TEXT
  )
`);
sqlite.exec(`
  CREATE TABLE IF NOT EXISTS bot_state_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
  )
`);

function cloneValue(value) {
  return JSON.parse(JSON.stringify(value));
}

function readStoreJson(key, fallback) {
  const row = sqlite.prepare("SELECT value FROM kv_store WHERE key = ?").get(key);
  if (!row) {
    return cloneValue(fallback);
  }

  try {
    return JSON.parse(row.value);
  } catch {
    return cloneValue(fallback);
  }
}

function writeStoreJson(key, value) {
  sqlite.prepare(`
    INSERT INTO kv_store (key, value)
    VALUES (?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
  `).run(key, JSON.stringify(value, null, 2));
}

function syncReadableTables(db, botState = null) {
  const write = sqlite.transaction(() => {
    sqlite.prepare("DELETE FROM tickets").run();
    sqlite.prepare("DELETE FROM ticket_messages").run();
    sqlite.prepare("DELETE FROM vouchers").run();
    sqlite.prepare("DELETE FROM live_visitors").run();
    sqlite.prepare("DELETE FROM payment_visits").run();
    sqlite.prepare("DELETE FROM bot_state_meta").run();

    const insertTicket = sqlite.prepare(`
      INSERT INTO tickets (
        id, name, topic, order_id, status, person_key, ip, user_agent,
        fingerprint, browser_name, platform, created_at, updated_at, resolved_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertTicketMessage = sqlite.prepare(`
      INSERT INTO ticket_messages (id, ticket_id, sender, body, created_at, sort_order)
      VALUES (?, ?, ?, ?, ?, ?)
    `);
    const insertVoucher = sqlite.prepare(`
      INSERT INTO vouchers (
        id, created_at, person_key, ip, user_agent, product_id, product_name,
        option_id, option_label, price, customer_name, customer_email,
        customer_notes, voucher_code, fingerprint, browser_name, platform
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertLiveVisitor = sqlite.prepare(`
      INSERT INTO live_visitors (
        person_key, ip, browser_name, platform, page, path_name, title, last_seen_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertPaymentVisit = sqlite.prepare(`
      INSERT INTO payment_visits (
        id, created_at, person_key, ip, user_agent, order_number, product_id,
        product_name, option_id, option_label, price, customer_name,
        customer_email, customer_notes, fingerprint, browser_name, platform
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertBotMeta = sqlite.prepare(`
      INSERT INTO bot_state_meta (key, value) VALUES (?, ?)
    `);

    for (const ticket of db.tickets || []) {
      insertTicket.run(
        ticket.id || "",
        ticket.name || "",
        ticket.topic || "",
        ticket.orderId || "",
        ticket.status || "",
        ticket.personKey || "",
        ticket.ip || "",
        ticket.userAgent || "",
        ticket.fingerprint || "",
        ticket.browserName || "",
        ticket.platform || "",
        ticket.createdAt || "",
        ticket.updatedAt || "",
        ticket.resolvedAt || ""
      );
      (ticket.messages || []).forEach((message, index) => {
        insertTicketMessage.run(
          message.id || `${ticket.id || "ticket"}:${index}`,
          ticket.id || "",
          message.sender || "",
          message.body || "",
          message.createdAt || "",
          index
        );
      });
    }

    for (const voucher of db.vouchers || []) {
      insertVoucher.run(
        voucher.id || "",
        voucher.createdAt || "",
        voucher.personKey || "",
        voucher.ip || "",
        voucher.userAgent || "",
        voucher.productId || "",
        voucher.productName || "",
        voucher.optionId || "",
        voucher.optionLabel || "",
        voucher.price || "",
        voucher.customerName || "",
        voucher.customerEmail || "",
        voucher.customerNotes || "",
        voucher.voucherCode || "",
        voucher.fingerprint || "",
        voucher.browserName || "",
        voucher.platform || ""
      );
    }

    for (const visitor of Object.values(db.liveVisitors || {})) {
      insertLiveVisitor.run(
        visitor.personKey || "",
        visitor.ip || "",
        visitor.browserName || "",
        visitor.platform || "",
        visitor.page || "",
        visitor.pathName || "",
        visitor.title || "",
        visitor.lastSeenAt || 0
      );
    }

    for (const visit of db.paymentVisits || []) {
      insertPaymentVisit.run(
        visit.id || "",
        visit.createdAt || "",
        visit.personKey || "",
        visit.ip || "",
        visit.userAgent || "",
        visit.orderNumber || "",
        visit.productId || "",
        visit.productName || "",
        visit.optionId || "",
        visit.optionLabel || "",
        visit.price || "",
        visit.customerName || "",
        visit.customerEmail || "",
        visit.customerNotes || "",
        visit.fingerprint || "",
        visit.browserName || "",
        visit.platform || ""
      );
    }

    const currentBotState = botState || readStoreJson("bot_state", DEFAULT_BOT_STATE);
    for (const [key, value] of Object.entries(currentBotState || {})) {
      insertBotMeta.run(key, JSON.stringify(value, null, 2));
    }
  });

  write();
}

function loadDb() {
  const parsed = readStoreJson("app_db", DEFAULT_APP_DB);
  const voucherLinks = loadVoucherLinksFromTable();
  return {
    tickets: Array.isArray(parsed.tickets) ? parsed.tickets : [],
    cooldowns: parsed.cooldowns || {},
    vouchers: Array.isArray(parsed.vouchers) ? parsed.vouchers : [],
    voucherLinks,
    voucherCooldowns: parsed.voucherCooldowns || {},
    liveVisitors: parsed.liveVisitors || {},
    paymentVisits: Array.isArray(parsed.paymentVisits) ? parsed.paymentVisits : [],
    paymentVisitCooldowns: parsed.paymentVisitCooldowns || {}
  };
}

function saveDb(db) {
  const { voucherLinks, ...rest } = db;
  writeStoreJson("app_db", rest);
  syncReadableTables(rest);
}

function normalizePriceKey(value) {
  const raw = String(value ?? "").trim().replace(/[^0-9.]/g, "");
  const numeric = Number(raw);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "";
  }

  return String(Number.isInteger(numeric) ? numeric : numeric);
}

function sanitizeVoucherLinks(links) {
  if (!Array.isArray(links)) {
    return [];
  }

  return links
    .map((item) => ({
      label: normalizeText(item?.label, 80),
      url: normalizeText(item?.url, 500)
    }))
    .filter((item) => item.label && item.url);
}

function mergeDefaultVoucherLinks(voucherLinks) {
  const merged = { ...DEFAULT_VOUCHER_LINKS };

  if (voucherLinks && typeof voucherLinks === "object") {
    for (const [priceKey, links] of Object.entries(voucherLinks)) {
      const cleanKey = normalizePriceKey(priceKey);
      if (!cleanKey) continue;
      const sanitized = sanitizeVoucherLinks(links);
      if (sanitized.length) {
        merged[cleanKey] = sanitized;
      }
    }
  }

  return merged;
}

function loadVoucherLinksFromTable() {
  const rows = sqlite.prepare(`
    SELECT price_key, sort_order, label, url
    FROM voucher_links
    ORDER BY CAST(price_key AS REAL), sort_order
  `).all();

  if (!rows.length) {
    return cloneValue(DEFAULT_VOUCHER_LINKS);
  }

  const grouped = {};
  for (const row of rows) {
    if (!grouped[row.price_key]) {
      grouped[row.price_key] = [];
    }
    grouped[row.price_key].push({
      label: row.label,
      url: row.url
    });
  }

  return grouped;
}

function replaceVoucherLinksTable(voucherLinks) {
  const insert = sqlite.prepare(`
    INSERT INTO voucher_links (price_key, sort_order, label, url)
    VALUES (?, ?, ?, ?)
  `);

  const write = sqlite.transaction((linksMap) => {
    sqlite.prepare("DELETE FROM voucher_links").run();
    for (const [priceKey, links] of Object.entries(linksMap)) {
      links.forEach((link, index) => {
        insert.run(priceKey, index, link.label, link.url);
      });
    }
  });

  write(voucherLinks);
}

function initializeVoucherLinksTable() {
  const countRow = sqlite.prepare("SELECT COUNT(*) AS count FROM voucher_links").get();
  const storedDb = readStoreJson("app_db", DEFAULT_APP_DB);
  const mergedLinks = mergeDefaultVoucherLinks(storedDb.voucherLinks || {});

  if (!countRow?.count) {
    replaceVoucherLinksTable(mergedLinks);
  }

  if (storedDb.voucherLinks && Object.keys(storedDb.voucherLinks).length) {
    storedDb.voucherLinks = {};
    writeStoreJson("app_db", storedDb);
  }
}

initializeVoucherLinksTable();
syncReadableTables(readStoreJson("app_db", DEFAULT_APP_DB), readStoreJson("bot_state", DEFAULT_BOT_STATE));

function loadBotState() {
  const parsed = readStoreJson("bot_state", DEFAULT_BOT_STATE);
  return {
    seen_voucher_ids: Array.isArray(parsed.seen_voucher_ids) ? parsed.seen_voucher_ids : [],
    seen_payment_visit_ids: Array.isArray(parsed.seen_payment_visit_ids) ? parsed.seen_payment_visit_ids : [],
    muted_ticket_ids: Array.isArray(parsed.muted_ticket_ids) ? parsed.muted_ticket_ids : [],
    pending_reply_ticket_id: typeof parsed.pending_reply_ticket_id === "string" ? parsed.pending_reply_ticket_id : ""
  };
}

function json(res, statusCode, payload, origin = "") {
  const headers = {
    "Content-Type": "application/json; charset=utf-8"
  };

  if (origin && origin === ALLOWED_ORIGIN) {
    headers["Access-Control-Allow-Origin"] = origin;
    headers["Vary"] = "Origin";
  }

  res.writeHead(statusCode, headers);
  res.end(JSON.stringify(payload));
}

function normalizeText(value, maxLength) {
  return String(value || "").trim().replace(/\s+/g, " ").slice(0, maxLength);
}

function escapeTelegram(value) {
  return String(value).replace(/[_*[\]()~`>#+\-=|{}.!]/g, "\\$&");
}

function getClientIp(req) {
  const forwarded = req.headers["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded.length > 0) {
    return forwarded.split(",")[0].trim();
  }

  return req.socket.remoteAddress || "unknown";
}

function isPublicIp(value) {
  const ip = String(value || "").trim().toLowerCase();
  if (!ip || ip === "unknown" || ip === "::1" || ip === "127.0.0.1") {
    return false;
  }

  if (ip.startsWith("::ffff:")) {
    return isPublicIp(ip.slice(7));
  }

  if (
    ip.startsWith("10.") ||
    ip.startsWith("192.168.") ||
    ip.startsWith("169.254.") ||
    /^172\.(1[6-9]|2\d|3[0-1])\./.test(ip) ||
    ip.startsWith("fc") ||
    ip.startsWith("fd") ||
    ip.startsWith("fe80:")
  ) {
    return false;
  }

  return true;
}

function makePersonKey(ip, fingerprint, userAgent) {
  const raw = `${ip}|${fingerprint}|${userAgent}`;
  return crypto.createHash("sha256").update(raw).digest("hex");
}

async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString("utf8");
  return raw ? JSON.parse(raw) : {};
}

async function sendTelegramSupportMessage(ticket, message, mode = "new") {
  const state = loadBotState();
  if (mode !== "new" && state.muted_ticket_ids.includes(ticket.id)) {
    return;
  }

  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  const header = mode === "followup" ? "Support Ticket Update" : "Support Ticket";
  const text = [
    `💬 *${escapeTelegram(header)}*`,
    "",
    `Name: ${escapeTelegram(ticket.name)}`,
    `Topic: ${escapeTelegram(ticket.topic)}`,
    `Order ID: ${escapeTelegram(ticket.orderId || "N/A")}`,
    `Message: ${escapeTelegram(message)}`
  ].join("\n");

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text,
      parse_mode: "MarkdownV2",
      reply_markup: {
        inline_keyboard: [
          [
            { text: "Reply", callback_data: `ticket:reply:${ticket.id}` },
            { text: "Close", callback_data: `ticket:close:${ticket.id}` },
            { text: "Mute", callback_data: `ticket:mute:${ticket.id}` }
          ]
        ]
      }
    })
  });

  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Telegram request failed: ${payload}`);
  }
}

async function sendTelegramVoucherMessage(voucher) {
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  const text = [
    "🔥 *Voucher Payment Alert*",
    "",
    `VOUCHER: \`${escapeTelegram(voucher.voucherCode)}\``,
    `Name: ${escapeTelegram(voucher.customerName || "N/A")}`,
    `Order number: ${escapeTelegram(voucher.orderNumber || "N/A")}`,
    `Email: ${escapeTelegram(voucher.customerEmail || "N/A")}`
  ].join("\n");

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text,
      parse_mode: "MarkdownV2"
    })
  });

  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Telegram request failed: ${payload}`);
  }
}

async function sendTelegramPaymentVisitMessage(visit) {
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  const text = [
    "*Payment Page Visitor*",
    `Order: ${escapeTelegram(visit.orderNumber || "N/A")}`,
    `Product: ${escapeTelegram(visit.productName || "Unknown")}`,
    `Option: ${escapeTelegram(visit.optionLabel || "Unknown")}`,
    `Price: ${escapeTelegram(String(visit.price || "N/A"))}`,
    `Name: ${escapeTelegram(visit.customerName || "N/A")}`,
    `Delivery Email: ${escapeTelegram(visit.customerEmail || "N/A")}`,
    `Notes: ${escapeTelegram(visit.customerNotes || "None")}`,
    `IP: \`${escapeTelegram(visit.ip)}\``,
    `Browser: ${escapeTelegram(visit.browserName || "Unknown")} / ${escapeTelegram(visit.platform || "Unknown")}`
  ].join("\n");

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text,
      parse_mode: "MarkdownV2"
    })
  });

  if (!response.ok) {
    const payload = await response.text();
    throw new Error(`Telegram request failed: ${payload}`);
  }
}

function validatePayload(payload) {
  const name = normalizeText(payload.name, 80);
  const topic = normalizeText(payload.topic, 80);
  const orderId = normalizeText(payload.orderId, 80);
  const message = String(payload.message || "").trim().slice(0, 2000);
  const fingerprint = normalizeText(payload.fingerprint, 200);
  const browserName = normalizeText(payload.browserName, 80);
  const platform = normalizeText(payload.platform, 80);
  const honeypot = normalizeText(payload.website, 120);

  if (honeypot) {
    return { error: "Spam rejected." };
  }

  if (!name || name.length < 2) {
    return { error: "Name is required." };
  }

  if (!topic) {
    return { error: "A support topic is required." };
  }

  if (!message || message.length < 10) {
    return { error: "Message is too short." };
  }

  if (!fingerprint) {
    return { error: "Browser fingerprint is missing." };
  }

  return {
    name,
    topic,
    orderId,
    message,
    fingerprint,
    browserName,
    platform
  };
}

function validateVoucherPayload(payload) {
  const productId = normalizeText(payload.productId, 80);
  const productName = normalizeText(payload.productName, 120);
  const optionId = normalizeText(payload.optionId, 80);
  const optionLabel = normalizeText(payload.optionLabel, 120);
  const price = normalizeText(payload.price, 40);
  const customerName = normalizeText(payload.customerName, 80);
  const customerEmail = normalizeText(payload.customerEmail, 160);
  const customerNotes = String(payload.customerNotes || "").trim().slice(0, 500);
  const voucherCode = normalizeText(payload.voucherCode, 120).replace(/\s+/g, "");
  const fingerprint = normalizeText(payload.fingerprint, 200);
  const browserName = normalizeText(payload.browserName, 80);
  const platform = normalizeText(payload.platform, 80);

  if (!productId || !productName) {
    return { error: "Product details are missing." };
  }

  if (!voucherCode || voucherCode.length !== 16) {
    return { error: "Voucher code must be 16 characters." };
  }

  if (!fingerprint) {
    return { error: "Browser fingerprint is missing." };
  }

  return {
    productId,
    productName,
    optionId,
    optionLabel,
    price,
    customerName,
    customerEmail,
    customerNotes,
    voucherCode,
    fingerprint,
    browserName,
    platform
  };
}

function validateCardCheckoutPayload(payload) {
  const productId = normalizeText(payload.productId, 80);
  const productName = normalizeText(payload.productName, 120);
  const optionId = normalizeText(payload.optionId, 80);
  const optionLabel = normalizeText(payload.optionLabel, 120);
  const priceRaw = String(payload.price ?? "").trim();
  const price = Number(priceRaw);
  const orderNumber = normalizeText(payload.orderNumber, 80);
  const customerName = normalizeText(payload.customerName, 80);
  const customerEmail = normalizeText(payload.customerEmail, 160);
  const customerNotes = String(payload.customerNotes || "").trim().slice(0, 500);

  if (!productId || !productName) {
    return { error: "Product details are missing." };
  }

  if (!Number.isFinite(price) || price <= 0) {
    return { error: "Price is invalid." };
  }

  if (!orderNumber) {
    return { error: "Order number is missing." };
  }

  return {
    productId,
    productName,
    optionId,
    optionLabel,
    price,
    orderNumber,
    customerName,
    customerEmail,
    customerNotes
  };
}

async function readJsonResponse(response) {
  const raw = await response.text();
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

function getHoodpayCheckoutUrl(payload) {
  const candidates = [
    payload?.url,
    payload?.checkoutUrl,
    payload?.checkout_url,
    payload?.paymentUrl,
    payload?.payment_url,
    payload?.redirectUrl,
    payload?.redirect_url,
    payload?.hostedUrl,
    payload?.hosted_url,
    payload?.data?.url,
    payload?.data?.checkoutUrl,
    payload?.data?.checkout_url,
    payload?.data?.paymentUrl,
    payload?.data?.payment_url,
    payload?.data?.redirectUrl,
    payload?.data?.redirect_url,
    payload?.data?.hostedUrl,
    payload?.data?.hosted_url
  ];

  for (const value of candidates) {
    const clean = typeof value === "string" ? value.trim() : "";
    if (/^https?:\/\//i.test(clean)) {
      return clean;
    }
  }

  return "";
}

async function createHoodpayCheckout(checkout, req) {
  const headers = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${HOODPAY_API_KEY}`,
    "x-api-key": HOODPAY_API_KEY,
    "x-business-id": HOODPAY_BUSINESS_ID
  };

  const body = {
    amount: checkout.price,
    currency: "USD",
    name: `${checkout.productName}${checkout.optionLabel ? ` - ${checkout.optionLabel}` : ""}`.slice(0, 120),
    description: `Order ${checkout.orderNumber}${checkout.customerNotes ? ` | ${checkout.customerNotes}` : ""}`.slice(0, 400),
    redirectUrl: HOODPAY_RETURN_URL || undefined,
    notifyUrl: HOODPAY_NOTIFY_URL || undefined,
    customerEmail: checkout.customerEmail || undefined,
    customerIp: isPublicIp(getClientIp(req)) ? getClientIp(req) : undefined,
    customerUserAgent: normalizeText(req.headers["user-agent"], 300) || undefined,
    metadata: {
      orderNumber: checkout.orderNumber,
      productId: checkout.productId,
      productName: checkout.productName,
      optionId: checkout.optionId,
      optionLabel: checkout.optionLabel,
      customerName: checkout.customerName,
      customerEmail: checkout.customerEmail
    }
  };

  const response = await fetch(HOODPAY_CREATE_PAYMENT_URL, {
    method: "POST",
    headers,
    body: JSON.stringify(body)
  });
  const payload = await readJsonResponse(response);

  if (!response.ok) {
    const detail = payload?.message || payload?.error || payload?.raw || `HTTP ${response.status}`;
    console.error("Hoodpay create payment failed", {
      status: response.status,
      detail,
      payload
    });
    throw new Error(String(detail));
  }

  const checkoutUrl = getHoodpayCheckoutUrl(payload);
  if (!checkoutUrl) {
    throw new Error("Hoodpay did not return a checkout URL.");
  }

  return {
    checkoutUrl,
    payload
  };
}

function validateLivePayload(payload) {
  const page = normalizeText(payload.page, 80);
  const pathName = normalizeText(payload.pathName, 200);
  const fingerprint = normalizeText(payload.fingerprint, 200);
  const browserName = normalizeText(payload.browserName, 80);
  const platform = normalizeText(payload.platform, 80);
  const title = normalizeText(payload.title, 120);

  if (!page) {
    return { error: "Page is required." };
  }

  if (!fingerprint) {
    return { error: "Browser fingerprint is missing." };
  }

  return {
    page,
    pathName,
    fingerprint,
    browserName,
    platform,
    title
  };
}

function validatePaymentVisitPayload(payload) {
  const orderNumber = normalizeText(payload.orderNumber, 20);
  const productId = normalizeText(payload.productId, 80);
  const productName = normalizeText(payload.productName, 120);
  const optionId = normalizeText(payload.optionId, 80);
  const optionLabel = normalizeText(payload.optionLabel, 120);
  const price = normalizeText(payload.price, 40);
  const customerName = normalizeText(payload.customerName, 80);
  const customerEmail = normalizeText(payload.customerEmail, 160);
  const customerNotes = String(payload.customerNotes || "").trim().slice(0, 500);
  const fingerprint = normalizeText(payload.fingerprint, 200);
  const browserName = normalizeText(payload.browserName, 80);
  const platform = normalizeText(payload.platform, 80);

  if (!productId || !productName) {
    return { error: "Product details are missing." };
  }

  if (!orderNumber) {
    return { error: "Order number is missing." };
  }

  if (!fingerprint) {
    return { error: "Browser fingerprint is missing." };
  }

  return {
    orderNumber,
    productId,
    productName,
    optionId,
    optionLabel,
    price,
    customerName,
    customerEmail,
    customerNotes,
    fingerprint,
    browserName,
    platform
  };
}

function validateSupportMessagePayload(payload) {
  const ticketId = normalizeText(payload.ticketId, 80);
  const message = String(payload.message || "").trim().slice(0, 2000);
  const fingerprint = normalizeText(payload.fingerprint, 200);
  const browserName = normalizeText(payload.browserName, 80);
  const platform = normalizeText(payload.platform, 80);

  if (!ticketId) {
    return { error: "Ticket ID is required." };
  }

  if (!message || message.length < 2) {
    return { error: "Message is too short." };
  }

  if (!fingerprint) {
    return { error: "Browser fingerprint is missing." };
  }

  return {
    ticketId,
    message,
    fingerprint,
    browserName,
    platform
  };
}

function sanitizeTicketForClient(ticket) {
  return {
    id: ticket.id,
    createdAt: ticket.createdAt,
    status: ticket.status,
    name: ticket.name,
    topic: ticket.topic,
    orderId: ticket.orderId || "",
    messages: Array.isArray(ticket.messages) ? ticket.messages : []
  };
}

function pruneCooldowns(db, now) {
  for (const [key, value] of Object.entries(db.cooldowns)) {
    if (now - value > 60_000) {
      delete db.cooldowns[key];
    }
  }
}

function pruneTimestampMap(map, now, maxAgeMs) {
  for (const [key, value] of Object.entries(map)) {
    if (now - value > maxAgeMs) {
      delete map[key];
    }
  }
}

function pruneLiveVisitors(db, now) {
  for (const [key, visitor] of Object.entries(db.liveVisitors)) {
    if (!visitor?.lastSeenAt || now - visitor.lastSeenAt > 90_000) {
      delete db.liveVisitors[key];
    }
  }
}

http
  .createServer(async (req, res) => {
    const origin = typeof req.headers.origin === "string" ? req.headers.origin : "";

    if (req.method === "OPTIONS") {
      if (origin === ALLOWED_ORIGIN) {
        res.writeHead(204, {
          "Access-Control-Allow-Origin": origin,
          "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
          Vary: "Origin"
        });
        res.end();
        return;
      }

      res.writeHead(403);
      res.end();
      return;
    }

    if (req.url?.startsWith("/health")) {
      json(res, 200, { ok: true, service: "support-backend" }, origin);
      return;
    }

    if (req.method === "GET" && req.url?.startsWith("/api/support/thread")) {
      if (!ALLOWED_ORIGIN) {
        json(res, 500, { error: "Server is not configured." }, origin);
        return;
      }

      if (origin !== ALLOWED_ORIGIN) {
        json(res, 403, { error: "Origin not allowed." }, origin);
        return;
      }

      const requestUrl = new URL(req.url, "http://localhost");
      const ticketId = normalizeText(requestUrl.searchParams.get("ticketId"), 80);
      const fingerprint = normalizeText(requestUrl.searchParams.get("fingerprint"), 200);

      if (!ticketId || !fingerprint) {
        json(res, 400, { error: "Ticket ID and fingerprint are required." }, origin);
        return;
      }

      const ip = getClientIp(req);
      const userAgent = normalizeText(req.headers["user-agent"], 300);
      const personKey = makePersonKey(ip, fingerprint, userAgent);
      const db = loadDb();
      const ticket = db.tickets.find((item) => item.id === ticketId);

      if (!ticket || ticket.personKey !== personKey) {
        json(res, 404, { error: "Ticket not found." }, origin);
        return;
      }

      json(res, 200, { ok: true, ticket: sanitizeTicketForClient(ticket) }, origin);
      return;
    }

    if (req.method === "GET" && req.url?.startsWith("/api/voucher-links")) {
      if (!ALLOWED_ORIGIN) {
        json(res, 500, { error: "Server is not configured." }, origin);
        return;
      }

      if (origin !== ALLOWED_ORIGIN) {
        json(res, 403, { error: "Origin not allowed." }, origin);
        return;
      }

      const requestUrl = new URL(req.url, "http://localhost");
      const priceKey = normalizePriceKey(requestUrl.searchParams.get("price"));

      if (!priceKey) {
        json(res, 400, { error: "Price is required." }, origin);
        return;
      }

      const links = sanitizeVoucherLinks(loadVoucherLinksFromTable()?.[priceKey]);
      json(res, 200, { ok: true, price: priceKey, links }, origin);
      return;
    }

    if (req.method === "POST" && req.url === "/api/live-heartbeat") {
      if (!ALLOWED_ORIGIN) {
        json(res, 500, { error: "Server is not configured." }, origin);
        return;
      }

      if (origin !== ALLOWED_ORIGIN) {
        json(res, 403, { error: "Origin not allowed." }, origin);
        return;
      }

      let payload;
      try {
        payload = await readBody(req);
      } catch {
        json(res, 400, { error: "Invalid JSON payload." }, origin);
        return;
      }

      const validated = validateLivePayload(payload);
      if ("error" in validated) {
        json(res, 400, { error: validated.error }, origin);
        return;
      }

      const ip = getClientIp(req);
      const userAgent = normalizeText(req.headers["user-agent"], 300);
      const personKey = makePersonKey(ip, validated.fingerprint, userAgent);
      const db = loadDb();
      const now = Date.now();

      pruneLiveVisitors(db, now);
      db.liveVisitors[personKey] = {
        personKey,
        ip,
        browserName: validated.browserName,
        platform: validated.platform,
        page: validated.page,
        pathName: validated.pathName,
        title: validated.title,
        lastSeenAt: now
      };
      saveDb(db);

      json(res, 200, { ok: true, liveCount: Object.keys(db.liveVisitors).length }, origin);
      return;
    }

    if (req.method === "POST" && req.url === "/api/support") {
      if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID || !ALLOWED_ORIGIN) {
        json(res, 500, { error: "Server is not configured." }, origin);
        return;
      }

      if (origin !== ALLOWED_ORIGIN) {
        json(res, 403, { error: "Origin not allowed." }, origin);
        return;
      }

      let payload;
      try {
        payload = await readBody(req);
      } catch {
        json(res, 400, { error: "Invalid JSON payload." }, origin);
        return;
      }

      const validated = validatePayload(payload);
      if ("error" in validated) {
        json(res, 400, { error: validated.error }, origin);
        return;
      }

      const ip = getClientIp(req);
      const userAgent = normalizeText(req.headers["user-agent"], 300);
      const personKey = makePersonKey(ip, validated.fingerprint, userAgent);

      const db = loadDb();
      const now = Date.now();
      pruneCooldowns(db, now);

      const cooldownUntil = db.cooldowns[personKey] || 0;
      if (cooldownUntil > now) {
        json(res, 429, { error: "Please wait 3 seconds before sending again." }, origin);
        return;
      }

      const hasOpenTicket = db.tickets.some(
        (ticket) => ticket.personKey === personKey && ticket.status === "open"
      );

      if (hasOpenTicket) {
        json(res, 409, { error: "Only one open ticket is allowed per person." }, origin);
        return;
      }

      const ticket = {
        id: crypto.randomUUID(),
        createdAt: new Date(now).toISOString(),
        status: "open",
        personKey,
        ip,
        userAgent,
        messages: [
          {
            id: crypto.randomUUID(),
            sender: "user",
            body: validated.message,
            createdAt: new Date(now).toISOString()
          }
        ],
        ...validated
      };

      try {
        await sendTelegramSupportMessage(ticket, validated.message, "new");
      } catch (error) {
        json(res, 502, { error: "Telegram delivery failed.", detail: String(error.message || error) }, origin);
        return;
      }

      db.tickets.push(ticket);
      db.cooldowns[personKey] = now + 3000;
      saveDb(db);

      json(res, 200, { ok: true, ticketId: ticket.id, ticket: sanitizeTicketForClient(ticket) }, origin);
      return;
    }

    if (req.method === "POST" && req.url === "/api/support/message") {
      if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID || !ALLOWED_ORIGIN) {
        json(res, 500, { error: "Server is not configured." }, origin);
        return;
      }

      if (origin !== ALLOWED_ORIGIN) {
        json(res, 403, { error: "Origin not allowed." }, origin);
        return;
      }

      let payload;
      try {
        payload = await readBody(req);
      } catch {
        json(res, 400, { error: "Invalid JSON payload." }, origin);
        return;
      }

      const validated = validateSupportMessagePayload(payload);
      if ("error" in validated) {
        json(res, 400, { error: validated.error }, origin);
        return;
      }

      const ip = getClientIp(req);
      const userAgent = normalizeText(req.headers["user-agent"], 300);
      const personKey = makePersonKey(ip, validated.fingerprint, userAgent);
      const db = loadDb();
      const now = Date.now();
      pruneCooldowns(db, now);

      const cooldownUntil = db.cooldowns[personKey] || 0;
      if (cooldownUntil > now) {
        json(res, 429, { error: "Please wait 3 seconds before sending again." }, origin);
        return;
      }

      const ticket = db.tickets.find((item) => item.id === validated.ticketId);
      if (!ticket || ticket.personKey !== personKey) {
        json(res, 404, { error: "Ticket not found." }, origin);
        return;
      }

      if (ticket.status !== "open") {
        json(res, 409, { error: "This ticket is already closed." }, origin);
        return;
      }

      ticket.messages = Array.isArray(ticket.messages) ? ticket.messages : [];
      ticket.messages.push({
        id: crypto.randomUUID(),
        sender: "user",
        body: validated.message,
        createdAt: new Date(now).toISOString()
      });
      ticket.browserName = validated.browserName || ticket.browserName;
      ticket.platform = validated.platform || ticket.platform;
      ticket.updatedAt = new Date(now).toISOString();

      try {
        await sendTelegramSupportMessage(ticket, validated.message, "followup");
      } catch (error) {
        json(res, 502, { error: "Telegram delivery failed.", detail: String(error.message || error) }, origin);
        return;
      }

      db.cooldowns[personKey] = now + 3000;
      saveDb(db);

      json(res, 200, { ok: true, ticket: sanitizeTicketForClient(ticket) }, origin);
      return;
    }

    if (req.method === "POST" && req.url === "/api/voucher") {
      if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID || !ALLOWED_ORIGIN) {
        json(res, 500, { error: "Server is not configured." }, origin);
        return;
      }

      if (origin !== ALLOWED_ORIGIN) {
        json(res, 403, { error: "Origin not allowed." }, origin);
        return;
      }

      let payload;
      try {
        payload = await readBody(req);
      } catch {
        json(res, 400, { error: "Invalid JSON payload." }, origin);
        return;
      }

      const validated = validateVoucherPayload(payload);
      if ("error" in validated) {
        json(res, 400, { error: validated.error }, origin);
        return;
      }

      const ip = getClientIp(req);
      const userAgent = normalizeText(req.headers["user-agent"], 300);
      const personKey = makePersonKey(ip, validated.fingerprint, userAgent);

      const db = loadDb();
      const now = Date.now();
      pruneCooldowns({ cooldowns: db.voucherCooldowns }, now);

      const cooldownUntil = db.voucherCooldowns[personKey] || 0;
      if (cooldownUntil > now) {
        json(res, 429, { error: "Please wait 3 seconds before submitting another voucher." }, origin);
        return;
      }

      const voucher = {
        id: crypto.randomUUID(),
        createdAt: new Date(now).toISOString(),
        personKey,
        ip,
        userAgent,
        ...validated
      };

      try {
        await sendTelegramVoucherMessage(voucher);
      } catch (error) {
        json(res, 502, { error: "Telegram delivery failed.", detail: String(error.message || error) }, origin);
        return;
      }

      db.vouchers.push(voucher);
      db.voucherCooldowns[personKey] = now + 3000;
      saveDb(db);

      json(res, 200, { ok: true, voucherId: voucher.id }, origin);
      return;
    }

    if (req.method === "POST" && req.url === "/api/card-checkout") {
      if (!ALLOWED_ORIGIN) {
        json(res, 500, { error: "Server is not configured." }, origin);
        return;
      }

      if (!HOODPAY_CREATE_PAYMENT_URL || !HOODPAY_API_KEY || !HOODPAY_BUSINESS_ID) {
        json(res, 500, { error: "Hoodpay is not configured on the server." }, origin);
        return;
      }

      if (origin !== ALLOWED_ORIGIN) {
        json(res, 403, { error: "Origin not allowed." }, origin);
        return;
      }

      let payload;
      try {
        payload = await readBody(req);
      } catch {
        json(res, 400, { error: "Invalid JSON payload." }, origin);
        return;
      }

      const validated = validateCardCheckoutPayload(payload);
      if ("error" in validated) {
        json(res, 400, { error: validated.error }, origin);
        return;
      }

      try {
        const hoodpay = await createHoodpayCheckout(validated, req);
        json(res, 200, { ok: true, checkoutUrl: hoodpay.checkoutUrl }, origin);
      } catch (error) {
        json(res, 502, { error: "Could not create Hoodpay checkout.", detail: String(error.message || error) }, origin);
      }
      return;
    }

    if (req.method === "POST" && req.url === "/api/payment-visit") {
      if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID || !ALLOWED_ORIGIN) {
        json(res, 500, { error: "Server is not configured." }, origin);
        return;
      }

      if (origin !== ALLOWED_ORIGIN) {
        json(res, 403, { error: "Origin not allowed." }, origin);
        return;
      }

      let payload;
      try {
        payload = await readBody(req);
      } catch {
        json(res, 400, { error: "Invalid JSON payload." }, origin);
        return;
      }

      const validated = validatePaymentVisitPayload(payload);
      if ("error" in validated) {
        json(res, 400, { error: validated.error }, origin);
        return;
      }

      const ip = getClientIp(req);
      const userAgent = normalizeText(req.headers["user-agent"], 300);
      const personKey = makePersonKey(ip, validated.fingerprint, userAgent);
      const db = loadDb();
      const now = Date.now();
      const notificationKey = `${personKey}:${validated.orderNumber}`;

      pruneTimestampMap(db.paymentVisitCooldowns, now, 30 * 60_000);

      const visit = {
        id: crypto.randomUUID(),
        createdAt: new Date(now).toISOString(),
        personKey,
        ip,
        userAgent,
        ...validated
      };

      db.paymentVisits.push(visit);

      if (!db.paymentVisitCooldowns[notificationKey]) {
        try {
          await sendTelegramPaymentVisitMessage(visit);
          db.paymentVisitCooldowns[notificationKey] = now;
        } catch (error) {
          json(res, 502, { error: "Telegram delivery failed.", detail: String(error.message || error) }, origin);
          return;
        }
      }

      saveDb(db);
      json(res, 200, { ok: true, paymentVisitId: visit.id }, origin);
      return;
    }

    if (req.method === "POST" && req.url?.startsWith("/api/resolve/")) {
      const token = new URL(req.url, "http://localhost").searchParams.get("token") || "";
      if (!ADMIN_RESOLVE_TOKEN || token !== ADMIN_RESOLVE_TOKEN) {
        json(res, 403, { error: "Forbidden." }, origin);
        return;
      }

      const ticketId = req.url.split("/api/resolve/")[1]?.split("?")[0];
      const db = loadDb();
      const ticket = db.tickets.find((item) => item.id === ticketId);

      if (!ticket) {
        json(res, 404, { error: "Ticket not found." }, origin);
        return;
      }

      ticket.status = "resolved";
      ticket.resolvedAt = new Date().toISOString();
      saveDb(db);
      json(res, 200, { ok: true }, origin);
      return;
    }

    json(res, 404, { error: "Not found." }, origin);
  })
  .listen(PORT, () => {
    console.log(`Support backend listening on port ${PORT}`);
  });
