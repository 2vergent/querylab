import { useMemo, useState } from "react";
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

  return {
    totalExecutionTime: root.derived.effectiveTime,
    worstSelf,
    worstMismatch,
    totalDiskReads,
    hasSpill,
    maxJoinAmplification,
  };
}

function getInsightSeverityForNode(nodeId, insights) {
  const matches = insights.filter((item) => item.nodeId === nodeId);
  if (matches.length === 0) return null;
  return matches.reduce((best, item) => {
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
  const [searchNodeType, setSearchNodeType] = useState("");
  const [treeRenderSeed, setTreeRenderSeed] = useState(0);
  const [defaultOpen, setDefaultOpen] = useState(true);

  const summary = useMemo(() => (tree ? buildSummary(tree) : null), [tree]);

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
    } catch (error) {
      setTree(null);
      setInsights([]);
      setParseError(error.message || "Unable to parse plan.");
    }
  }

  return (
    <div className="app-shell">
      <header className="app-header">
        <h1>PostgreSQL Execution Analysis Instrument</h1>
        <p>Deterministic execution plan parsing, metric derivation, and engineering-grade insights.</p>
      </header>

      <section className="panel input-panel">
        <textarea
          rows={12}
          value={input}
          onChange={(event) => setInput(event.target.value)}
          placeholder="Paste EXPLAIN ANALYZE output (JSON or text)..."
        />
        <div className="input-actions">
          <button onClick={analyze}>Analyze Plan</button>
        </div>
        {parseError && <div className="error-banner">{parseError}</div>}
      </section>

      {tree && summary && (
        <>
          <section className="panel controls-panel">
            <div className="control-row">
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
            <div className="control-row">
              <input
                value={searchNodeType}
                onChange={(event) => setSearchNodeType(event.target.value)}
                placeholder="Search node type..."
              />
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
          </section>

          <section className="panel summary-grid">
            <SummaryCard title="Total Execution Time" value={`${formatNumber(summary.totalExecutionTime)} ms`} />
            <SummaryCard
              title="Worst Self Time Node"
              value={
                summary.worstSelf
                  ? `${summary.worstSelf.nodeType} (${formatNumber(summary.worstSelf.derived.selfTime)} ms)`
                  : "N/A"
              }
            />
            <SummaryCard
              title="Worst Estimate Mismatch"
              value={
                summary.worstMismatch
                  ? `${summary.worstMismatch.node.nodeType} (${formatNumber(summary.worstMismatch.score)}x)`
                  : "N/A"
              }
            />
            <SummaryCard title="Total Disk Reads" value={formatNumber(summary.totalDiskReads, 0)} />
            <SummaryCard title="Spill Indicator" value={summary.hasSpill ? "Spill Detected" : "No Spill"} />
            <SummaryCard
              title="Join Amplification"
              value={`${formatNumber(summary.maxJoinAmplification)}x max`}
            />
          </section>

          <section className="panel">
            <h2>Insights</h2>
            <div className="insight-list">
              {insights.map((insight, index) => (
                <article key={`${insight.nodeId}-${index}`} className={`insight insight-${insight.severity}`}>
                  <div className="insight-header">
                    <span className={`badge badge-${insight.severity}`}>{insight.severity.toUpperCase()}</span>
                    <span className="category">{insight.category}</span>
                    <h3>{insight.title}</h3>
                  </div>
                  <p>{insight.explanation}</p>
                  <p>
                    <strong>Recommendation:</strong> {insight.recommendation}
                  </p>
                </article>
              ))}
              {insights.length === 0 && <div className="empty">No rule triggers on this plan.</div>}
            </div>
          </section>

          <section className="panel">
            <h2>Execution Tree</h2>
            <TreeNode
              key={`${tree.id}-${treeRenderSeed}`}
              node={tree}
              rootMetricMax={Math.max(1, useSelfTime ? tree.derived.selfTime : tree.derived.effectiveTime)}
              defaultOpen={defaultOpen}
              useSelfTime={useSelfTime}
              highlightMismatch={highlightMismatch}
              showEstVsActual={showEstVsActual}
              searchNodeType={searchNodeType}
              insights={insights}
              renderSeed={treeRenderSeed}
            />
          </section>
        </>
      )}
    </div>
  );
}

function SummaryCard({ title, value }) {
  return (
    <article className="summary-card">
      <h3>{title}</h3>
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
  searchNodeType,
  insights,
  renderSeed,
}) {
  const [open, setOpen] = useState(defaultOpen);

  const metricValue = useSelfTime ? node.derived.selfTime : node.derived.effectiveTime;
  const ratio = Math.min(1, metricValue / Math.max(1, rootMetricMax));
  const mismatchRatio = node.derived.rowEstimateRatio;
  const foldedMismatch =
    !Number.isFinite(mismatchRatio) || mismatchRatio <= 0
      ? Number.POSITIVE_INFINITY
      : Math.max(mismatchRatio, 1 / mismatchRatio);
  const isMismatch = foldedMismatch > 3;
  const isHighSelf = node.derived.selfTime > 0.2 * Math.max(1, rootMetricMax);
  const normalizedSearch = searchNodeType.trim().toLowerCase();
  const subtreeMatches = (candidate) => {
    if (!normalizedSearch) return true;
    if (candidate.nodeType.toLowerCase().includes(normalizedSearch)) return true;
    return candidate.children.some(subtreeMatches);
  };
  const matchesSearch = subtreeMatches(node);
  const severity = getInsightSeverityForNode(node.id, insights);

  if (!matchesSearch) {
    return null;
  }

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
          </div>
          {showEstVsActual && (
            <div className="node-metrics">
              est rows {formatNumber(node.estimatedRows, 0)} | actual rows {formatNumber(node.actual.rows, 0)} | loops{" "}
              {formatNumber(node.actual.loops, 0)}
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
              searchNodeType={searchNodeType}
              insights={insights}
              renderSeed={renderSeed}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export default App;
