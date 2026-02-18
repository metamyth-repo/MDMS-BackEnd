// Backend/train_baseline.js
const fs = require("fs");
const path = require("path");
const readline = require("readline");

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
  toJSON() {
    return { n: this.n, mean: this.mean, M2: this.M2 };
  }
  static fromJSON(o) {
    const rs = new RunningStats();
    rs.n = Number(o?.n || 0);
    rs.mean = Number(o?.mean || 0);
    rs.M2 = Number(o?.M2 || 0);
    return rs;
  }
}

function bucket(val, step) {
  if (typeof val !== "number" || !Number.isFinite(val)) return null;
  return Math.round(val / step) * step;
}
function avg(arr) {
  const xs = arr.filter((x) => typeof x === "number" && Number.isFinite(x));
  if (!xs.length) return null;
  return xs.reduce((a, b) => a + b, 0) / xs.length;
}
function pick(measurements, name) {
  return measurements.filter((m) => m?.name === name).map((m) => m.value);
}

function parseArgs(argv) {
  const out = {
    files: [],
    outPath: "mdms_model.json",
    powerBucketKw: 1.0,
    flowBucket: 1.0,
  };

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--out") out.outPath = argv[++i];
    else if (a === "--power-bucket") out.powerBucketKw = Number(argv[++i] || 1);
    else if (a === "--flow-bucket") out.flowBucket = Number(argv[++i] || 1);
    else out.files.push(a);
  }
  return out;
}

function ensureDevice(model, deviceId) {
  if (model.devices[deviceId]) return model.devices[deviceId];
  model.devices[deviceId] = {
    // conditioned (kept for compatibility)
    tempByPower: {},
    vibByPower: {},
    pressByFlow: {},

    // NEW: unconditioned vibration baseline
    vibUncond: new RunningStats().toJSON(),

    counts: { rows: 0, samples: 0 },
  };
  return model.devices[deviceId];
}

function getStats(mapObj, bucketKey) {
  if (!mapObj[bucketKey]) mapObj[bucketKey] = new RunningStats().toJSON();
  return RunningStats.fromJSON(mapObj[bucketKey]);
}

async function trainOneFile(model, filePath, args) {
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const s = line.trim();
    if (!s) continue;

    let row;
    try {
      row = JSON.parse(s);
    } catch {
      continue;
    }

    const deviceId = row?.deviceId;
    const measurements = Array.isArray(row?.measurements) ? row.measurements : [];
    if (!deviceId || !measurements.length) continue;

    const dev = ensureDevice(model, deviceId);
    dev.counts.rows++;

    // allowed signals
    const power = avg(pick(measurements, "electrical.power"));
    const temp = avg(pick(measurements, "temperature"));
    const vib = avg(pick(measurements, "vibration_rms"));
    const flow = avg(pick(measurements, "fluid.flow"));
    const pressure = avg(pick(measurements, "fluid.pressure"));

    // NEW: unconditioned vibration baseline always updates if vib exists
    if (vib != null) {
      const rs = RunningStats.fromJSON(dev.vibUncond);
      rs.add(vib);
      dev.vibUncond = rs.toJSON();
      dev.counts.samples++;
    }

    // conditioned rules (only if you have those signals)
    if (power != null && temp != null) {
      const b = bucket(power, args.powerBucketKw);
      if (b != null) {
        const key = String(b);
        const rs = getStats(dev.tempByPower, key);
        rs.add(temp);
        dev.tempByPower[key] = rs.toJSON();
        dev.counts.samples++;
      }
    }

    if (power != null && vib != null) {
      const b = bucket(power, args.powerBucketKw);
      if (b != null) {
        const key = String(b);
        const rs = getStats(dev.vibByPower, key);
        rs.add(vib);
        dev.vibByPower[key] = rs.toJSON();
        dev.counts.samples++;
      }
    }

    if (flow != null && pressure != null) {
      const b = bucket(flow, args.flowBucket);
      if (b != null) {
        const key = String(b);
        const rs = getStats(dev.pressByFlow, key);
        rs.add(pressure);
        dev.pressByFlow[key] = rs.toJSON();
        dev.counts.samples++;
      }
    }
  }
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.files.length) {
    console.error("Usage: node train_baseline.js <file1.ndjson> <file2.ndjson> ... --out mdms_model.json");
    process.exit(2);
  }

  const files = args.files.map((f) => path.resolve(f));
  const outPath = path.resolve(args.outPath);

  const model = {
    meta: {
      createdAt: new Date().toISOString(),
      powerBucketKw: args.powerBucketKw,
      flowBucket: args.flowBucket,
      trainedFromFiles: files,
    },
    devices: {},
  };

  const t0 = Date.now();
  for (const f of files) {
    if (!fs.existsSync(f)) {
      console.warn(`[train] skip missing file: ${f}`);
      continue;
    }
    await trainOneFile(model, f, args);
  }

  fs.writeFileSync(outPath, JSON.stringify(model, null, 2));
  const dt = ((Date.now() - t0) / 1000).toFixed(2);

  const deviceCount = Object.keys(model.devices).length;
  console.log(`[train] files=${files.length} devices=${deviceCount} wrote=${outPath} elapsed=${dt}s`);

  for (const [devId, dev] of Object.entries(model.devices)) {
    const vib = RunningStats.fromJSON(dev.vibUncond);
    console.log(
      `[train] device=${devId} rows=${dev.counts.rows} samples=${dev.counts.samples} ` +
        `vibUncond_n=${vib.n} vibUncond_mean=${vib.mean.toFixed(3)} vibUncond_std=${vib.std().toFixed(3)} ` +
        `tempBuckets=${Object.keys(dev.tempByPower).length} vibBuckets=${Object.keys(dev.vibByPower).length} flowBuckets=${Object.keys(dev.pressByFlow).length}`
    );
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});