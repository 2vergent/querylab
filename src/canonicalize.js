import { v4 as uuid } from "uuid";

function asNumber(value, fallback = 0) {
  if (value === undefined || value === null || value === "") return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function normalizeText(value) {
  if (value === undefined || value === null || value === "") return null;
  return String(value);
}

function pickFirst(raw, keys) {
  for (const key of keys) {
    if (raw[key] !== undefined && raw[key] !== null) return raw[key];
  }
  return null;
}

export default function canonicalize(node) {
  function transform(n) {
    const raw = n || {};
    const children = raw.Plans || raw.children || [];
    const filter = normalizeText(raw["Filter"]);
    const hashCond = normalizeText(raw["Hash Cond"]);
    const joinFilter = normalizeText(raw["Join Filter"]);
    const recheckCond = normalizeText(raw["Recheck Cond"]);
    const joinCond =
      hashCond || joinFilter || normalizeText(raw["Merge Cond"]) || normalizeText(raw["Index Cond"]);

    return {
      id: uuid(),
      nodeType: raw["Node Type"] || raw.nodeType || "Unknown",
      relationName: normalizeText(raw["Relation Name"] || raw.relationName),
      indexName: normalizeText(raw["Index Name"] || raw.indexName),
      raw,
      cost: {
        startup: asNumber(raw["Startup Cost"] ?? raw.cost?.startup, 0),
        total: asNumber(raw["Total Cost"] ?? raw.cost?.total, 0),
      },
      actual: {
        startupTime: asNumber(raw["Actual Startup Time"] ?? raw.actual?.startupTime, 0),
        totalTime: asNumber(raw["Actual Total Time"] ?? raw.actual?.totalTime, 0),
        rows: asNumber(raw["Actual Rows"] ?? raw.actual?.rows, 0),
        loops: asNumber(raw["Actual Loops"] ?? raw.actual?.loops, 1),
      },
      estimatedRows: asNumber(raw["Plan Rows"] ?? raw.estimatedRows, 0),
      width: asNumber(raw["Plan Width"] ?? raw.width, 0),
      buffers: {
        sharedHit: asNumber(pickFirst(raw, ["Shared Hit Blocks", "sharedHit"]), 0),
        sharedRead: asNumber(pickFirst(raw, ["Shared Read Blocks", "sharedRead"]), 0),
        sharedWritten: asNumber(pickFirst(raw, ["Shared Written Blocks", "sharedWritten"]), 0),
        sharedDirtied: asNumber(pickFirst(raw, ["Shared Dirtied Blocks", "sharedDirtied"]), 0),
        localHit: asNumber(pickFirst(raw, ["Local Hit Blocks", "localHit"]), 0),
        localRead: asNumber(pickFirst(raw, ["Local Read Blocks", "localRead"]), 0),
        localWritten: asNumber(pickFirst(raw, ["Local Written Blocks", "localWritten"]), 0),
        localDirtied: asNumber(pickFirst(raw, ["Local Dirtied Blocks", "localDirtied"]), 0),
        tempRead: asNumber(pickFirst(raw, ["Temp Read Blocks", "tempRead"]), 0),
        tempWritten: asNumber(pickFirst(raw, ["Temp Written Blocks", "tempWritten"]), 0),
        tempDirtied: asNumber(pickFirst(raw, ["Temp Dirtied Blocks", "tempDirtied"]), 0),
      },
      workers: {
        planned: asNumber(raw["Workers Planned"] ?? raw.workers?.planned, 0),
        launched: asNumber(raw["Workers Launched"] ?? raw.workers?.launched, 0),
      },
      metadata: {
        filter,
        joinCond,
        hashCond,
        joinFilter,
        recheckCond,
        sortMethod: normalizeText(raw["Sort Method"] || raw.metadata?.sortMethod),
        sortSpaceUsed: asNumber(raw["Sort Space Used"] ?? raw.metadata?.sortSpaceUsed, 0),
        sortSpaceType: normalizeText(raw["Sort Space Type"] || raw.metadata?.sortSpaceType),
        heapFetches: asNumber(raw["Heap Fetches"] ?? raw.metadata?.heapFetches, 0),
      },
      children: children.map(transform),
      derived: {},
    };
  }

  return transform(node);
}
