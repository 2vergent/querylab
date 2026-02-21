import { useEffect, useMemo, useRef, useState } from "react";
import parseInput from "./parser";
import canonicalize from "./canonicalize";
import statsEngine from "./statsEngine";
import ruleEngine from "./ruleEngine";
import "./App.css";

const severityRank = {
  high: 3,
  medium: 2,
  low: 1,
};

function flattenTree(root) {
  const nodes = [];
  function walk(node) {
    nodes.push(node);
    node.children.forEach(walk);
  }
  walk(root);
  return nodes;
}

function formatNumber(value, decimals = 2) {
  if (!Number.isFinite(value)) return "Infinity";
  return Number(value).toLocaleString(undefined, {
    maximumFractionDigits: decimals,
  });
}

function buildSummary(root) {
  const nodes = flattenTree(root);
  const worstSelf = nodes.reduce(
    (best, node) => (node.derived.selfTime > (best?.derived.selfTime ?? -1) ? node : best),
    null,
  );
  const worstMismatch = nodes.reduce((best, node) => {
    const ratio = node.derived.rowEstimateRatio;
    const folded = !Number.isFinite(ratio) || ratio <= 0 ? Number.POSITIVE_INFINITY : Math.max(ratio, 1 / ratio);
    const bestRatio = best?.score ?? -1;
    return folded > bestRatio ? { node, score: folded } : best;
  }, null);
  const totalDiskReads = nodes.reduce((sum, node) => sum + node.derived.totalBuffersRead, 0);
  const hasSpill = nodes.some((node) => node.derived.diskSpillDetected);
  const joins = nodes.filter((node) => /join|nested loop/i.test(node.nodeType));
  const maxJoinAmplification = joins.reduce(
    (max, node) => Math.max(max, node.derived.amplificationFactor || 0),
    0,
  );
  const sharedReads = nodes.reduce((sum, node) => sum + node.buffers.sharedRead, 0);
  const tempWrites = nodes.reduce((sum, node) => sum + node.buffers.tempWritten, 0);
  const totalNodes = nodes.length;
  const leafNodes = nodes.filter((node) => node.children.length === 0).length;
  const seqScanCount = nodes.filter((node) => node.nodeType === "Seq Scan").length;
  const worstTimeShare = nodes.reduce(
    (best, node) => (node.derived.timePercent > (best?.derived.timePercent ?? -1) ? node : best),
    null,
  );
  const estimateErrors = nodes.map((node) => {
    const ratio = node.derived.rowEstimateRatio;
    return !Number.isFinite(ratio) || ratio <= 0 ? Number.POSITIVE_INFINITY : Math.max(ratio, 1 / ratio);
  });
  const finiteErrors = estimateErrors.filter((value) => Number.isFinite(value));
  const avgEstimateError =
    finiteErrors.length > 0 ? finiteErrors.reduce((sum, value) => sum + value, 0) / finiteErrors.length : Infinity;
  const parallelPlanned = nodes.reduce((sum, node) => sum + node.workers.planned, 0);
  const parallelLaunched = nodes.reduce((sum, node) => sum + node.workers.launched, 0);
  const parallelUtilization = parallelPlanned > 0 ? parallelLaunched / parallelPlanned : 1;
  const totalRowsProcessed = nodes.reduce((sum, node) => sum + node.actual.rows * node.actual.loops, 0);
  const rowThroughput =
    root.derived.effectiveTime > 0 ? totalRowsProcessed / (root.derived.effectiveTime / 1000) : 0;

  return {
    totalExecutionTime: root.derived.effectiveTime,
    worstSelf,
    worstMismatch,
    totalDiskReads,
    hasSpill,
    maxJoinAmplification,
    sharedReads,
    tempWrites,
    totalNodes,
    leafNodes,
    seqScanCount,
    worstTimeShare,
    avgEstimateError,
    parallelUtilization,
    totalRowsProcessed,
    rowThroughput,
  };
}

function extractColumns(text) {
  const source = String(text || "");
  const matches = source.match(/[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*/g) || [];
  return Array.from(new Set(matches));
}

function explainNode(node) {
  const type = node.nodeType || "Operator";
  if (/Seq Scan/i.test(type)) {
    return "Sequential scan reads table pages directly and evaluates predicates row by row.";
  }
  if (/Index Scan|Index Only Scan/i.test(type)) {
    return "Index-driven access path uses key lookups instead of scanning full table pages.";
  }
  if (/Nested Loop/i.test(type)) {
    return "Nested loop joins each outer row with matching inner rows; expensive when inner side is not selective.";
  }
  if (/Hash Join/i.test(type)) {
    return "Hash join builds a hash table on one input and probes it with rows from the other input.";
  }
  if (/Sort/i.test(type)) {
    return "Sort orders rows for ORDER BY, merge joins, or grouped operations.";
  }
  if (/Aggregate|GroupAggregate/i.test(type)) {
    return "Aggregate computes grouped or global summaries from input rows.";
  }
  return "Execution operator participating in the query plan.";
}

function formatMs(value) {
  return `${formatNumber(value)} ms`;
}

function buildNodeTypeStats(root) {
  const nodes = flattenTree(root);
  const map = new Map();
  for (const node of nodes) {
    const key = node.nodeType || "Unknown";
    if (!map.has(key)) {
      map.set(key, {
        nodeType: key,
        count: 0,
        selfTime: 0,
        effectiveTime: 0,
      });
    }
    const row = map.get(key);
    row.count += 1;
    row.selfTime += node.derived.selfTime || 0;
    row.effectiveTime += node.derived.effectiveTime || 0;
  }
  return Array.from(map.values()).sort((a, b) => b.selfTime - a.selfTime);
}

function buildRelationStats(root) {
  const nodes = flattenTree(root).filter((node) => node.relationName);
  const map = new Map();
  for (const node of nodes) {
    const key = node.relationName;
    if (!map.has(key)) {
      map.set(key, {
        relationName: key,
        scanCount: 0,
        totalTime: 0,
        nodeTypes: new Set(),
      });
    }
    const row = map.get(key);
    row.scanCount += /scan/i.test(node.nodeType) ? 1 : 0;
    row.totalTime += node.derived.selfTime || 0;
    row.nodeTypes.add(node.nodeType);
  }
  return Array.from(map.values())
    .map((row) => ({ ...row, nodeTypes: Array.from(row.nodeTypes) }))
    .sort((a, b) => b.totalTime - a.totalTime);
}

function toDisplayValue(value) {
  if (Array.isArray(value)) return value.join(", ");
  if (value && typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function getNodeSqlDetails(node) {
  const raw = node.raw || {};
  const details = [];

  if (node.metadata.groupKey?.length > 0) details.push({ label: "Group By", value: node.metadata.groupKey.join(", ") });
  if (node.metadata.sortKey?.length > 0) details.push({ label: "Sort Key", value: node.metadata.sortKey.join(", ") });
  if (node.metadata.indexCond) details.push({ label: "Index Condition", value: node.metadata.indexCond });
  if (raw["Filter"] || node.metadata.filter) details.push({ label: "Filter", value: raw["Filter"] || node.metadata.filter });
  if (raw["Hash Cond"] || node.metadata.hashCond) details.push({ label: "Hash Condition", value: raw["Hash Cond"] || node.metadata.hashCond });
  if (raw["Join Filter"] || node.metadata.joinFilter) details.push({ label: "Join Filter", value: raw["Join Filter"] || node.metadata.joinFilter });
  if (raw["Recheck Cond"] || node.metadata.recheckCond) details.push({ label: "Recheck Condition", value: raw["Recheck Cond"] || node.metadata.recheckCond });
  if (node.metadata.mergeCond) details.push({ label: "Merge Condition", value: node.metadata.mergeCond });
  if (raw["Rows Removed by Filter"] !== undefined) {
    details.push({ label: "Rows Removed by Filter", value: formatNumber(Number(raw["Rows Removed by Filter"]), 0) });
  }
  if (node.metadata.rowsRemovedByIndexRecheck > 0) {
    details.push({ label: "Rows Removed by Index Recheck", value: formatNumber(node.metadata.rowsRemovedByIndexRecheck, 0) });
  }
  if (node.metadata.cacheKey) details.push({ label: "Cache Key", value: node.metadata.cacheKey });
  if (node.metadata.cacheMode) details.push({ label: "Cache Mode", value: node.metadata.cacheMode });
  if (node.metadata.cacheHits || node.metadata.cacheMisses || node.metadata.cacheEvictions || node.metadata.cacheOverflows) {
    details.push({
      label: "Cache Stats",
      value: `hits=${formatNumber(node.metadata.cacheHits, 0)}, misses=${formatNumber(node.metadata.cacheMisses, 0)}, evictions=${formatNumber(node.metadata.cacheEvictions, 0)}, overflows=${formatNumber(node.metadata.cacheOverflows, 0)}`,
    });
  }
  if (node.metadata.peakMemoryUsage > 0) {
    details.push({ label: "Peak Memory Usage", value: `${formatNumber(node.metadata.peakMemoryUsage, 0)} kB` });
  }
  if (node.metadata.hashBuckets > 0 || node.metadata.hashBatches > 0) {
    details.push({
      label: "Hash Storage",
      value: `buckets=${formatNumber(node.metadata.hashBuckets, 0)}, batches=${formatNumber(node.metadata.hashBatches, 0)}`,
    });
  }

  const sortMethod = node.metadata.sortMethod || raw["Sort Method"];
  if (sortMethod) {
    const sortSpaceType = node.metadata.sortSpaceType || raw["Sort Space Type"] || "Unknown";
    const sortSpaceUsed = node.metadata.sortSpaceUsed || raw["Sort Space Used"] || 0;
    details.push({
      label: "Sort Strategy",
      value: `${sortMethod} (${sortSpaceType}${sortSpaceUsed ? `, ${formatNumber(sortSpaceUsed, 0)} kB` : ""})`,
    });
  }

  const columnSource = details.map((item) => item.value).join(" ");
  const referencedColumns = extractColumns(columnSource);
  const rawKeyValues = Object.entries(raw)
    .filter(([key]) => !["Plans", "Workers", "children"].includes(key))
    .map(([key, value]) => ({ key, value: toDisplayValue(value) }));

  return {
    explanation: explainNode(node),
    details,
    referencedColumns,
    rawKeyValues,
  };
}

function getInsightSeverityForNode(nodeId, insights) {
  if (!insights || insights.length === 0) return null;
  return insights.reduce((best, item) => {
    if (!best) return item.severity;
    return severityRank[item.severity] > severityRank[best] ? item.severity : best;
  }, null);
}

function App() {
  const [input, setInput] = useState("");
  const [tree, setTree] = useState(null);
  const [insights, setInsights] = useState([]);
  const [parseError, setParseError] = useState("");
  const [useSelfTime, setUseSelfTime] = useState(false);
  const [highlightMismatch, setHighlightMismatch] = useState(true);
  const [showEstVsActual, setShowEstVsActual] = useState(true);
  const [treeRenderSeed, setTreeRenderSeed] = useState(0);
  const [defaultOpen, setDefaultOpen] = useState(true);
  const [scrollToStatsPending, setScrollToStatsPending] = useState(false);
  const statsSectionRef = useRef(null);

  const summary = useMemo(() => (tree ? buildSummary(tree) : null), [tree]);
  const nodeTypeStats = useMemo(() => (tree ? buildNodeTypeStats(tree) : []), [tree]);
  const relationStats = useMemo(() => (tree ? buildRelationStats(tree) : []), [tree]);
  const insightsByNode = useMemo(() => {
    const map = new Map();
    for (const insight of insights) {
      if (!map.has(insight.nodeId)) map.set(insight.nodeId, []);
      map.get(insight.nodeId).push(insight);
    }
    return map;
  }, [insights]);

  useEffect(() => {
    if (!scrollToStatsPending || !tree || !statsSectionRef.current) return;
    statsSectionRef.current.scrollIntoView({ behavior: "smooth", block: "start" });
    setScrollToStatsPending(false);
  }, [scrollToStatsPending, tree]);

  function analyze() {
    setParseError("");
    try {
      const parsed = parseInput(input);
      if (!parsed) {
        throw new Error("No executable plan detected.");
      }
      const canonical = canonicalize(parsed);
      statsEngine(canonical);
      const nextInsights = ruleEngine(canonical);

      setTree(canonical);
      setInsights(nextInsights);
      setScrollToStatsPending(true);
    } catch (error) {
      setTree(null);
      setInsights([]);
      setParseError(error.message || "Unable to parse plan.");
      setScrollToStatsPending(false);
    }
  }

  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="header-row">
          <div className="brand-line">
            <span className="brand-banner">Query Lab</span>
            <span className="brand-tagline">PostgreSQL plan insights</span>
          </div>
        </div>
      </header>

      <section className="panel input-panel">
        <div className="input-panel-header">
          <h2>Plan Input</h2>
          <span>Paste `EXPLAIN ANALYZE` output (JSON or text)</span>
        </div>
        <div className="input-editor-wrap">
        <textarea
          rows={12}
          value={input}
          onChange={(event) => setInput(event.target.value)}
          placeholder="Paste EXPLAIN ANALYZE output (JSON or text)..."
        />
        </div>
        <div className="input-actions">
          <button onClick={analyze}>Analyze Plan</button>
        </div>
        {parseError && <div className="error-banner">{parseError}</div>}
      </section>

      {tree && summary && (
        <>
          <section ref={statsSectionRef} className="panel summary-grid">
            <SummaryCard
              title="Total Execution Time"
              value={`${formatNumber(summary.totalExecutionTime)} ms`}
              description="Total inclusive runtime for the entire plan tree."
            />
            <SummaryCard
              title="Worst Self Time Node"
              value={
                summary.worstSelf
                  ? `${summary.worstSelf.nodeType} (${formatNumber(summary.worstSelf.derived.selfTime)} ms)`
                  : "N/A"
              }
              description="Operator spending the most exclusive (self) time."
            />
            <SummaryCard
              title="Worst Estimate Mismatch"
              value={
                summary.worstMismatch
                  ? `${summary.worstMismatch.node.nodeType} (${formatNumber(summary.worstMismatch.score)}x)`
                  : "N/A"
              }
              description="Largest divergence between planner estimate and actual rows."
            />
            <SummaryCard
              title="Total Disk Reads"
              value={formatNumber(summary.totalDiskReads, 0)}
              description="Total read blocks across shared/local/temp buffers."
            />
            <SummaryCard
              title="Spill Indicator"
              value={summary.hasSpill ? "Spill Detected" : "No Spill"}
              description="Flags sort/temp disk spill activity in any node."
            />
            <SummaryCard
              title="Join Amplification"
              value={`${formatNumber(summary.maxJoinAmplification)}x max`}
              description="Maximum child-row-to-output-row amplification among join nodes."
            />
            <SummaryCard
              title="Shared Read Blocks"
              value={formatNumber(summary.sharedReads, 0)}
              description="Physical shared-buffer reads from storage."
            />
            <SummaryCard
              title="Temp Written Blocks"
              value={formatNumber(summary.tempWrites, 0)}
              description="Temporary blocks written, usually from spills or hashing."
            />
            <SummaryCard
              title="Plan Shape"
              value={`${formatNumber(summary.totalNodes, 0)} nodes / ${formatNumber(summary.leafNodes, 0)} leaves`}
              description="Operator count and leaf-node count in this execution tree."
            />
            <SummaryCard
              title="Seq Scan Count"
              value={formatNumber(summary.seqScanCount, 0)}
              description="Number of sequential scan operators in the plan."
            />
            <SummaryCard
              title="Hotspot Share"
              value={
                summary.worstTimeShare
                  ? `${summary.worstTimeShare.nodeType} (${formatNumber(summary.worstTimeShare.derived.timePercent * 100)}%)`
                  : "N/A"
              }
              description="Node consuming the highest share of total inclusive time."
            />
            <SummaryCard
              title="Avg Estimate Error"
              value={`${formatNumber(summary.avgEstimateError)}x`}
              description="Average row estimate error factor across nodes."
            />
            <SummaryCard
              title="Parallel Utilization"
              value={`${formatNumber(summary.parallelUtilization * 100)}%`}
              description="Total launched workers divided by total planned workers."
            />
            <SummaryCard
              title="Total Rows Processed"
              value={formatNumber(summary.totalRowsProcessed, 0)}
              description="Sum of actual rows multiplied by loops across all plan nodes."
            />
            <SummaryCard
              title="Row Throughput"
              value={`${formatNumber(summary.rowThroughput, 0)} rows/s`}
              description="Estimated processing speed based on rows processed over total execution time."
            />
          </section>

          <section className="panel">
            <div className="execution-header">
              <h2>Execution Tree</h2>
              <div className="execution-actions">
                <button
                  type="button"
                  onClick={() => {
                    setDefaultOpen(true);
                    setTreeRenderSeed((value) => value + 1);
                  }}
                >
                  Expand All
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setDefaultOpen(false);
                    setTreeRenderSeed((value) => value + 1);
                  }}
                >
                  Collapse All
                </button>
              </div>
            </div>
            <div className="execution-controls">
              <label>
                <input
                  type="checkbox"
                  checked={useSelfTime}
                  onChange={(event) => setUseSelfTime(event.target.checked)}
                />
                Show self time instead of total time
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={highlightMismatch}
                  onChange={(event) => setHighlightMismatch(event.target.checked)}
                />
                Highlight row estimate mismatch
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={showEstVsActual}
                  onChange={(event) => setShowEstVsActual(event.target.checked)}
                />
                Show estimated vs actual
              </label>
            </div>
            <TreeNode
              key={`${tree.id}-${treeRenderSeed}`}
              node={tree}
              rootMetricMax={Math.max(1, useSelfTime ? tree.derived.selfTime : tree.derived.effectiveTime)}
              defaultOpen={defaultOpen}
              useSelfTime={useSelfTime}
              highlightMismatch={highlightMismatch}
              showEstVsActual={showEstVsActual}
              insightsByNode={insightsByNode}
              renderSeed={treeRenderSeed}
            />
          </section>

          <section className="panel panel-table">
            <h2>Statistics by Node Type</h2>
            <table className="stats-table">
              <thead>
                <tr>
                  <th align="left">Node Type</th>
                  <th align="left">Count</th>
                  <th align="left">Exclusive Time</th>
                  <th align="left">% of Query (exclusive)</th>
                </tr>
              </thead>
              <tbody>
                {nodeTypeStats.map((row) => (
                  <tr key={row.nodeType}>
                    <td>{row.nodeType}</td>
                    <td>{row.count}</td>
                    <td>{formatMs(row.selfTime)}</td>
                    <td>{formatNumber((row.selfTime / Math.max(1, summary.totalExecutionTime)) * 100)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          <section className="panel panel-table">
            <h2>Statistics by Relation</h2>
            <table className="stats-table">
              <thead>
                <tr>
                  <th align="left">Relation</th>
                  <th align="left">Scan Count</th>
                  <th align="left">Total Exclusive Time</th>
                  <th align="left">Node Types</th>
                </tr>
              </thead>
              <tbody>
                {relationStats.map((row) => (
                  <tr key={row.relationName}>
                    <td>{row.relationName}</td>
                    <td>{row.scanCount}</td>
                    <td>{formatMs(row.totalTime)}</td>
                    <td>{row.nodeTypes.join(", ")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </>
      )}
    </div>
  );
}

function SummaryCard({ title, value, description }) {
  return (
    <article className="summary-card">
      <div className="summary-card-header">
        <h3>{title}</h3>
        <span className="info-dot" title={description}>
          i
        </span>
      </div>
      <p>{value}</p>
    </article>
  );
}

function TreeNode({
  node,
  rootMetricMax,
  defaultOpen,
  useSelfTime,
  highlightMismatch,
  showEstVsActual,
  insightsByNode,
  renderSeed,
}) {
  const [open, setOpen] = useState(defaultOpen);
  const [showInsights, setShowInsights] = useState(false);
  const [showDetails, setShowDetails] = useState(false);

  const metricValue = useSelfTime ? node.derived.selfTime : node.derived.effectiveTime;
  const ratio = Math.min(1, metricValue / Math.max(1, rootMetricMax));
  const mismatchRatio = node.derived.rowEstimateRatio;
  const foldedMismatch =
    !Number.isFinite(mismatchRatio) || mismatchRatio <= 0
      ? Number.POSITIVE_INFINITY
      : Math.max(mismatchRatio, 1 / mismatchRatio);
  const isMismatch = foldedMismatch > 3;
  const isHighSelf = node.derived.selfTime > 0.2 * Math.max(1, rootMetricMax);
  const nodeInsights = insightsByNode.get(node.id) || [];
  const severity = getInsightSeverityForNode(node.id, nodeInsights);
  const sqlDetails = getNodeSqlDetails(node);
  const rowRatio =
    node.estimatedRows > 0 ? node.actual.rows / node.estimatedRows : node.actual.rows > 0 ? Number.POSITIVE_INFINITY : 1;
  const estimateError = Math.max(rowRatio, 1 / Math.max(rowRatio, 1e-9));
  const performanceSignals = [
    { label: "Inclusive", value: formatMs(node.derived.effectiveTime) },
    { label: "Exclusive", value: formatMs(node.derived.selfTime) },
    { label: "Time Share", value: `${formatNumber(node.derived.timePercent * 100)}%` },
    { label: "Estimate Error", value: `${formatNumber(estimateError)}x` },
    { label: "Amplification", value: `${formatNumber(node.derived.amplificationFactor || 0)}x` },
    { label: "Buffers Read", value: formatNumber(node.derived.totalBuffersRead, 0) },
    { label: "Disk Spill", value: node.derived.diskSpillDetected ? "Yes" : "No" },
    {
      label: "Workers",
      value: `${formatNumber(node.workers.launched, 0)} / ${formatNumber(node.workers.planned, 0)}`,
    },
  ];

  const tooltip =
    `actual rows: ${formatNumber(node.actual.rows, 0)}\n` +
    `estimated rows: ${formatNumber(node.estimatedRows, 0)}\n` +
    `loops: ${formatNumber(node.actual.loops, 0)}\n` +
    `buffers shared hit/read: ${formatNumber(node.buffers.sharedHit, 0)}/${formatNumber(node.buffers.sharedRead, 0)}\n` +
    `buffers temp read/written: ${formatNumber(node.buffers.tempRead, 0)}/${formatNumber(node.buffers.tempWritten, 0)}\n` +
    `effective time: ${formatNumber(node.derived.effectiveTime)} ms`;

  return (
    <div className="tree-node">
      <div
        className={[
          "tree-row",
          isHighSelf ? "high-self" : "",
          highlightMismatch && isMismatch ? "mismatch" : "",
          severity ? `severity-${severity}` : "",
        ].join(" ")}
        title={tooltip}
      >
        <button className="toggle" onClick={() => setOpen((value) => !value)} disabled={node.children.length === 0}>
          {node.children.length === 0 ? "." : open ? "-" : "+"}
        </button>
        <div className="node-main">
          <div className="node-title">
            <strong>{node.nodeType}</strong>
            {node.relationName && <span> on {node.relationName}</span>}
            {node.indexName && <span> using {node.indexName}</span>}
            {severity && <span className={`badge badge-${severity}`}>{severity.toUpperCase()}</span>}
            <button
              type="button"
              onClick={() => setShowDetails((value) => !value)}
              className="node-action-btn"
              title="Show SQL-level node details such as filters, join/sort keys, and referenced columns."
            >
              {showDetails ? "Hide Details" : "Details"}
            </button>
            {nodeInsights.length > 0 && (
              <button
                type="button"
                onClick={() => setShowInsights((value) => !value)}
                className="node-action-btn"
                title="Show optimizer findings for this node and why they matter."
              >
                {showInsights ? "Hide Insights" : `Insights (${nodeInsights.length})`}
              </button>
            )}
          </div>
          {showEstVsActual && (
            <div className="node-metrics">
              est rows {formatNumber(node.estimatedRows, 0)} | actual rows {formatNumber(node.actual.rows, 0)} | loops{" "}
              {formatNumber(node.actual.loops, 0)} | row ratio {formatNumber(rowRatio)}
            </div>
          )}
        </div>
        <div className="metric">
          {formatNumber(metricValue)} ms
          <div className="metric-percent">{formatNumber(node.derived.timePercent * 100)}%</div>
        </div>
      </div>

      <div className="bar-wrap">
        <div className="bar-fill" style={{ width: `${ratio * 100}%` }} />
      </div>

      {showDetails && (
        <div className="node-details">
          <div className="node-details-summary">
            <strong>Operator Summary:</strong> {sqlDetails.explanation}
          </div>
          {sqlDetails.referencedColumns.length > 0 && (
            <div className="node-details-columns">
              <strong>Referenced Columns:</strong> {sqlDetails.referencedColumns.join(", ")}
            </div>
          )}
          {sqlDetails.details.length > 0 ? (
            <div className="node-kv-grid">
              {sqlDetails.details.map((item, index) => (
                <div key={`${item.label}-${index}`} className="node-kv-row">
                  <strong>{item.label}:</strong> {String(item.value)}
                </div>
              ))}
            </div>
          ) : (
            <div className="node-kv-empty">No additional SQL-level metadata reported for this node.</div>
          )}
          <div className="signal-grid">
            {performanceSignals.map((signal) => (
              <div key={signal.label} className="signal-card">
                <span>{signal.label}</span>
                <strong>{signal.value}</strong>
              </div>
            ))}
          </div>
          <details className="raw-details">
            <summary>
              Raw Node Properties ({sqlDetails.rawKeyValues.length})
            </summary>
            <div className="node-kv-grid raw-grid">
              {sqlDetails.rawKeyValues.map((entry) => (
                <div key={entry.key} className="node-kv-row">
                  <strong>{entry.key}:</strong> {entry.value}
                </div>
              ))}
            </div>
          </details>
        </div>
      )}

      {showInsights && nodeInsights.length > 0 && (
        <div className="insight-list node-insights-list">
          {nodeInsights.map((insight, index) => (
            <article key={`${insight.nodeId}-${index}`} className={`insight insight-${insight.severity}`}>
              <div className="insight-header">
                <span className={`badge badge-${insight.severity}`}>{insight.severity.toUpperCase()}</span>
                <span className="category">{insight.category}</span>
                <h3>{insight.title}</h3>
              </div>
              <p title="What this means for the execution behavior of this node.">{insight.explanation}</p>
              <p>
                <strong title="Actionable SQL or indexing step to improve this node.">Recommendation:</strong>{" "}
                {insight.recommendation}
              </p>
            </article>
          ))}
        </div>
      )}

      {open && node.children.length > 0 && (
        <div className="children">
          {node.children.map((child) => (
            <TreeNode
              key={`${child.id}-${renderSeed}`}
              node={child}
              rootMetricMax={rootMetricMax}
              defaultOpen={defaultOpen}
              useSelfTime={useSelfTime}
              highlightMismatch={highlightMismatch}
              showEstVsActual={showEstVsActual}
              insightsByNode={insightsByNode}
              renderSeed={renderSeed}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export default App;
