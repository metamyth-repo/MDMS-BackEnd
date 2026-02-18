// Backend/backend_server.js
const express = require("express");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const http = require("http");
const WebSocket = require("ws");

const HOST = "0.0.0.0";
const PORT = 5002;
const WEBUI_URL = "http://0.0.0.0:5001";
const MODEL_PATH = process.env.MODEL_PATH || path.join(__dirname, "mdms_model.json");

const app = express();
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});
app.use(express.json({ limit: "10mb" }));

const BASE_DIR = __dirname;
const DEVICES_PATH = path.join(BASE_DIR, "mdms_devices.json");
const LOG_DIR = path.join(BASE_DIR, "mdms_logs");
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

// -------------------- detector config --------------------
const DETECTOR = {
  // conditioned
  warmupTotal: 60,
  minBucket: 15,
  zThreshold: 6,

  // NEW: vibration-only
  vibUncondWarmup: 200,      // require N vib samples before alerting
  vibUncondZThreshold: 6,    // |z| threshold

  powerBucketKw: 1.0,
  flowBucket: 1.0,

  epsStd: 1e-6,

  includeSnapshot: true,
  factsOnly: true,
  ultraMinimal: false,
};

// -------------------- utils --------------------
function readJsonSafe(p, fallback) {
  try {
    if (!fs.existsSync(p)) return fallback;
    const s = fs.readFileSync(p, "utf8");
    if (!s.trim()) return fallback;
    return JSON.parse(s);
  } catch {
    return fallback;
  }
}
function writeJsonAtomic(p, obj) {
  const tmp = `${p}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(obj, null, 2));
  fs.renameSync(tmp, p);
}
function nowIso() {
  return new Date().toISOString();
}
function newId() {
  return crypto.randomBytes(8).toString("hex");
}
function sanitizeName(s) {
  const t = String(s ?? "").trim();
  return t.length ? t.slice(0, 80) : "device";
}
function devicesLoad() {
  const data = readJsonSafe(DEVICES_PATH, { devices: [] });
  if (!data || !Array.isArray(data.devices)) return { devices: [] };
  return data;
}
function devicesSave(data) {
  writeJsonAtomic(DEVICES_PATH, data);
}
function findDevice(data, deviceId) {
  return data.devices.find((d) => d.deviceId === deviceId) || null;
}
function logPathFor(deviceId) {
  return path.join(LOG_DIR, `${deviceId}.ndjson`);
}
function appendLogs(deviceId, rows) {
  const p = logPathFor(deviceId);
  const lines = rows.map((r) => JSON.stringify(r)).join("\n") + "\n";
  fs.appendFileSync(p, lines);
}
function readLastLines(filePath, maxLines) {
  if (!fs.existsSync(filePath)) return [];
  const buf = fs.readFileSync(filePath);
  const text = buf.toString("utf8");
  const lines = text.split("\n").filter(Boolean);
  return lines
    .slice(-maxLines)
    .map((s) => {
      try { return JSON.parse(s); } catch { return null; }
    })
    .filter(Boolean);
}
function keyOf(m) {
  const sid = typeof m.sensorId === "string" && m.sensorId ? m.sensorId : "";
  return `${m.name}::${sid}`;
}
function isMeasurement(x) {
  if (!x || typeof x !== "object") return false;
  if (typeof x.name !== "string" || !x.name.trim()) return false;
  if (typeof x.unit !== "string" || !x.unit.trim()) return false;
  if (typeof x.value !== "number" || !Number.isFinite(x.value)) return false;
  if (x.sensorId != null && typeof x.sensorId !== "string") return false;
  if (x.ts != null && typeof x.ts !== "string") return false;
  return true;
}
function mergeSensorsFromTelemetry(device, measures) {
  const map = new Map();
  for (const s of device.sensors || []) {
    const sid = typeof s.sensorId === "string" ? s.sensorId : "";
    map.set(`${s.name}::${sid}`, { name: s.name, unit: s.unit, sensorId: sid });
  }
  for (const m of measures) {
    const sid = typeof m.sensorId === "string" ? m.sensorId : "";
    map.set(`${m.name}::${sid}`, { name: m.name, unit: m.unit, sensorId: sid });
  }
  device.sensors = Array.from(map.values());
}
function computeLatest(deviceId) {
  const rows = readLastLines(logPathFor(deviceId), 2000);
  const latest = new Map();
  for (const r of rows) {
    if (!r || !r.measurements || !Array.isArray(r.measurements)) continue;
    for (const m of r.measurements) {
      if (!isMeasurement(m)) continue;
      latest.set(keyOf(m), m);
    }
  }
  return Array.from(latest.values());
}

// -------------------- websocket --------------------
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/ws" });

const wsClients = new Set();
wss.on("connection", (ws) => {
  wsClients.add(ws);
  ws.send(JSON.stringify({ type: "hello", serverTs: nowIso(), webui: WEBUI_URL }));
  ws.on("close", () => wsClients.delete(ws));
  ws.on("error", () => wsClients.delete(ws));
});
function wsBroadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const ws of wsClients) {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(msg); } catch {}
    }
  }
}

// -------------------- formatting --------------------
function severityFromZ(zAbs) {
  if (zAbs >= 20) return "critical";
  if (zAbs >= 10) return "high";
  if (zAbs >= 6) return "warning";
  return "info";
}
function fmt(n, digits = 2) {
  if (typeof n !== "number" || !Number.isFinite(n)) return null;
  return Number(n.toFixed(digits));
}
function signWord(x) {
  return x >= 0 ? "above" : "below";
}
function stripToFacts(anom) {
  if (!DETECTOR.factsOnly) return anom;
  const { likelyCauses, suggestedChecks, ...rest } = anom;
  if (!DETECTOR.ultraMinimal) return rest;
  const { model, rule, context, ...rest2 } = rest;
  return rest2;
}
function enrichFacts(a) {
  const z = typeof a.z === "number" ? a.z : NaN;
  const residual = typeof a.residual === "number" ? a.residual : NaN;
  const out = { ...a, severity: a.severity || severityFromZ(Math.abs(z)) };

  if (a.kind === "vibration_unconditioned_z") {
    out.explanation = `Vibration is ${fmt(Math.abs(residual), 2)} ${a.unit || ""} ${signWord(residual)} baseline (z=${fmt(z, 2)}).`;
  } else if (a.kind === "temperature_residual_vs_load") {
    out.explanation = `Temperature is ${fmt(Math.abs(residual), 2)}°C ${signWord(residual)} expected for load ≈ ${fmt(a.load, 2)} ${a.loadUnit || "kW"} (z=${fmt(z, 2)}).`;
  } else if (a.kind === "vibration_residual_vs_load") {
    out.explanation = `Vibration is ${fmt(Math.abs(residual), 3)} mm/s ${signWord(residual)} expected for load ≈ ${fmt(a.load, 2)} ${a.loadUnit || "kW"} (z=${fmt(z, 2)}).`;
  } else if (a.kind === "pressure_residual_vs_flow") {
    out.explanation = `Pressure is ${fmt(Math.abs(residual), 2)} bar ${signWord(residual)} expected for flow ≈ ${fmt(a.flow, 2)} ${a.flowUnit || "m3/h"} (z=${fmt(z, 2)}).`;
  }

  return out;
}

// -------------------- stats + models --------------------
class RunningStats {
  constructor() {
    this.n = 0;
    this.mean = 0;
    this.M2 = 0;
  }
  add(x) {
    this.n += 1;
    const delta = x - this.mean;
    this.mean += delta / this.n;
    const delta2 = x - this.mean;
    this.M2 += delta * delta2;
  }
  variance() {
    if (this.n < 2) return 0;
    return this.M2 / (this.n - 1);
  }
  std() {
    return Math.sqrt(Math.max(0, this.variance()));
  }
  static fromJSON(o) {
    const rs = new RunningStats();
    rs.n = Number(o?.n || 0);
    rs.mean = Number(o?.mean || 0);
    rs.M2 = Number(o?.M2 || 0);
    return rs;
  }
}

const models = new Map(); // deviceId -> state

function makeEmptyModel(deviceId) {
  return {
    deviceId,

    // conditioned stats (kept)
    totals: {
      temperature_residual_vs_load: 0,
      vibration_residual_vs_load: 0,
      pressure_residual_vs_flow: 0,
    },
    tempByPower: new Map(),
    vibByPower: new Map(),
    pressByFlow: new Map(),

    // NEW: vibration-only baseline stats
    vibUncond: new RunningStats(), // baseline mean/std
    vibUncondCount: 0,             // count seen in streaming for warmup

    last: {
      temperature: null,
      vibration_rms: null,
      fluid_pressure: null,
      electrical_power: null,
      electrical_current: null,
      fluid_flow: null,
    },
  };
}
function getModel(deviceId) {
  if (models.has(deviceId)) return models.get(deviceId);
  const m = makeEmptyModel(deviceId);
  models.set(deviceId, m);
  return m;
}

function bucket(val, step) {
  if (typeof val !== "number" || !Number.isFinite(val)) return null;
  return Math.round(val / step) * step;
}
function pickValues(meas, name) {
  return meas.filter((m) => m.name === name).map((m) => m.value);
}
function avg(arr) {
  const xs = arr.filter((x) => typeof x === "number" && Number.isFinite(x));
  if (!xs.length) return null;
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}
function computeResidual(stats, observed) {
  const expected = stats.mean;
  const std = Math.max(stats.std(), DETECTOR.epsStd);
  const residual = observed - expected;
  const z = residual / std;
  return { expected, std, residual, z, n: stats.n };
}

// -------------------- baseline loader --------------------
function loadBaselineModels() {
  if (!fs.existsSync(MODEL_PATH)) {
    console.log(`[model] no baseline at ${MODEL_PATH} (learn live)`);
    return;
  }
  const raw = readJsonSafe(MODEL_PATH, null);
  if (!raw || typeof raw !== "object") {
    console.log(`[model] failed to read baseline: ${MODEL_PATH}`);
    return;
  }

  const meta = raw.meta || {};
  if (Number.isFinite(Number(meta.powerBucketKw))) DETECTOR.powerBucketKw = Number(meta.powerBucketKw);
  if (Number.isFinite(Number(meta.flowBucket))) DETECTOR.flowBucket = Number(meta.flowBucket);

  const devicesObj = raw.devices || {};
  let devCount = 0;

  for (const [deviceId, dev] of Object.entries(devicesObj)) {
    const m = makeEmptyModel(deviceId);

    // NEW: vibration-only baseline
    if (dev?.vibUncond) {
      m.vibUncond = RunningStats.fromJSON(dev.vibUncond);
      // treat baseline as already "warmed" if it has enough samples
      m.vibUncondCount = m.vibUncond.n;
    }

    // Keep conditioned hydrations for compatibility
    for (const [bStr, statJson] of Object.entries(dev?.tempByPower || {})) {
      const b = Number(bStr);
      if (Number.isFinite(b)) m.tempByPower.set(b, RunningStats.fromJSON(statJson));
    }
    for (const [bStr, statJson] of Object.entries(dev?.vibByPower || {})) {
      const b = Number(bStr);
      if (Number.isFinite(b)) m.vibByPower.set(b, RunningStats.fromJSON(statJson));
    }
    for (const [bStr, statJson] of Object.entries(dev?.pressByFlow || {})) {
      const b = Number(bStr);
      if (Number.isFinite(b)) m.pressByFlow.set(b, RunningStats.fromJSON(statJson));
    }

    // allow conditioned rules to trigger immediately if baseline exists
    m.totals.temperature_residual_vs_load = DETECTOR.warmupTotal;
    m.totals.vibration_residual_vs_load = DETECTOR.warmupTotal;
    m.totals.pressure_residual_vs_flow = DETECTOR.warmupTotal;

    models.set(deviceId, m);
    devCount++;
  }

  console.log(
    `[model] loaded ${MODEL_PATH} devices=${devCount} powerBucketKw=${DETECTOR.powerBucketKw} flowBucket=${DETECTOR.flowBucket}`
  );
}

// -------------------- anomaly emit helpers --------------------
function emitAnomaly(obj) {
  const out = stripToFacts(enrichFacts(obj));
  console.log("[ANOMALY]", out);
  wsBroadcast(out);
}

function runDetectors(deviceId, ts, measurements) {
  const m = getModel(deviceId);

  // read values (only allowed names)
  const power = avg(pickValues(measurements, "electrical.power"));
  const current = avg(pickValues(measurements, "electrical.current"));
  const temp = avg(pickValues(measurements, "temperature"));
  const vib = avg(pickValues(measurements, "vibration_rms"));
  const flow = avg(pickValues(measurements, "fluid.flow"));
  const pressure = avg(pickValues(measurements, "fluid.pressure"));

  const snapshot = {
    electrical: { power, current },
    temperature: temp,
    vibration_rms: vib,
    fluid: { flow, pressure },
  };

  const lastVib = m.last.vibration_rms;
  m.last.vibration_rms = vib ?? m.last.vibration_rms;

  // ---------------- NEW: vibration-only anomaly ----------------
  if (typeof vib === "number") {
    m.vibUncondCount += 1;

    // baseline must exist (loaded) OR learn live if baseline empty
    // If no baseline loaded, we learn baseline for a while then alert.
    const baseline = m.vibUncond;
    const baselineReady = baseline.n >= DETECTOR.vibUncondWarmup;

    if (baselineReady) {
      const ri = computeResidual(baseline, vib);
      const zAbs = Math.abs(ri.z);
      if (zAbs >= DETECTOR.vibUncondZThreshold) {
        emitAnomaly({
          type: "anomaly",
          kind: "vibration_unconditioned_z",
          deviceId,
          ts,
          observed: vib,
          expected: ri.expected,
          residual: ri.residual,
          z: ri.z,
          unit: measurements.find(x => x.name === "vibration_rms")?.unit || "",
          model: { nBaseline: baseline.n, baselineStd: ri.std },
          context: DETECTOR.includeSnapshot ? { snapshot, last: { vibration_rms: lastVib } } : undefined,
          rule: { zThreshold: DETECTOR.vibUncondZThreshold, baselineMin: DETECTOR.vibUncondWarmup }
        });
      }
    }

    // Always keep learning baseline if baseline is not preloaded/ready:
    // (If you trained from file 01 and loaded, baselineReady will be true immediately)
    baseline.add(vib);
  }

  // NOTE: conditioned rules still exist in your previous versions.
  // With this vibration-only dataset they won't fire; keep if you want.

  return;
}

// -------------------- routes --------------------
app.get("/health", (req, res) => {
  res.json({ ok: true, name: "MDMS Backend", webui: WEBUI_URL, host: HOST, port: PORT, serverTs: nowIso() });
});

app.get("/api/devices", (req, res) => {
  const data = devicesLoad();
  res.json(data.devices.map((d) => ({ deviceId: d.deviceId, deviceName: d.deviceName, createdTs: d.createdTs })));
});

app.post("/api/device/register", (req, res) => {
  const data = devicesLoad();
  const deviceName = sanitizeName(req.body?.deviceName);
  const deviceId = newId();
  const device = { deviceId, deviceName, createdTs: nowIso(), sensors: [] };
  data.devices.push(device);
  devicesSave(data);
  res.json({ ok: true, deviceId, deviceName, serverTs: nowIso() });
});

app.get("/api/device/:deviceId/profile", (req, res) => {
  const deviceId = req.params.deviceId;
  const data = devicesLoad();
  const d = findDevice(data, deviceId);
  if (!d) return res.status(404).json({ error: "device not found" });
  res.json({ deviceId: d.deviceId, deviceName: d.deviceName, createdTs: d.createdTs, sensors: d.sensors || [] });
});

app.post("/api/device/:deviceId/telemetry", (req, res) => {
  const deviceId = req.params.deviceId;
  const data = devicesLoad();
  const d = findDevice(data, deviceId);
  if (!d) return res.status(404).json({ error: "device not found" });

  const body = req.body;
  const measuresIn = Array.isArray(body) ? body : Array.isArray(body?.measurements) ? body.measurements : [];
  const ts = nowIso();

  const cleaned = [];
  for (const m of measuresIn) {
    if (!isMeasurement(m)) continue;
    const name = m.name;
    const allowed =
      name === "vibration_rms" ||
      name === "electrical.current" ||
      name === "electrical.power" ||
      name === "fluid.flow" ||
      name === "fluid.pressure" ||
      name === "temperature";
    if (!allowed) continue;

    cleaned.push({
      ts: typeof m.ts === "string" && m.ts ? m.ts : ts,
      name,
      value: m.value,
      unit: m.unit,
      sensorId: typeof m.sensorId === "string" ? m.sensorId : "",
    });
  }

  if (cleaned.length === 0) return res.status(400).json({ error: "no valid measurements" });

  mergeSensorsFromTelemetry(d, cleaned);
  devicesSave(data);

  appendLogs(deviceId, [{ ts, deviceId, measurements: cleaned }]);
  wsBroadcast({ type: "telemetry", ts, deviceId, measurements: cleaned });

  runDetectors(deviceId, ts, cleaned);

  res.json({ ok: true, accepted: cleaned.length, serverTs: ts });
});

app.get("/api/device/:deviceId/latest", (req, res) => {
  const deviceId = req.params.deviceId;
  const data = devicesLoad();
  const d = findDevice(data, deviceId);
  if (!d) return res.status(404).json({ error: "device not found" });
  res.json(computeLatest(deviceId));
});

app.get("/api/device/:deviceId/logs", (req, res) => {
  const deviceId = req.params.deviceId;
  const n = Math.max(1, Math.min(5000, Number(req.query.n || 200)));
  const data = devicesLoad();
  const d = findDevice(data, deviceId);
  if (!d) return res.status(404).json({ error: "device not found" });
  const rows = readLastLines(logPathFor(deviceId), n);
  res.json({ ok: true, deviceId, count: rows.length, rows });
});

app.get("/api/model/status", (req, res) => {
  const deviceIds = Array.from(models.keys());
  const summary = deviceIds.map((id) => {
    const m = models.get(id);
    return {
      deviceId: id,
      vibUncond_n: m.vibUncond?.n || 0,
      vibUncond_mean: m.vibUncond?.mean || 0,
      vibUncond_std: m.vibUncond?.std ? m.vibUncond.std() : 0,
    };
  });
  res.json({ ok: true, modelPath: MODEL_PATH, devices: summary });
});

// -------------------- start --------------------
loadBaselineModels();

server.listen(PORT, HOST, () => {
  console.log(`MDMS Backend: http://${HOST}:${PORT} (webui expected at ${WEBUI_URL})`);
  console.log(`WS endpoint: ws://127.0.0.1:${PORT}/ws`);
});