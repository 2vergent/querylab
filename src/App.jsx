import { useMemo, useRef, useState } from "react";
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
function getFoldedEstimateErrorFromRatio(ratio) {
  if (!Number.isFinite(ratio) || ratio <= 0) return Number.POSITIVE_INFINITY;
  return Math.max(ratio, 1 / ratio);
}

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
    const folded = getFoldedEstimateErrorFromRatio(ratio);
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
    return getFoldedEstimateErrorFromRatio(ratio);
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
  const relationCount = new Set(
    nodes.map((node) => node.relationName).filter(Boolean),
  ).size;
  const complexityScore = totalNodes + (maxJoinAmplification > 10 ? 5 : 0) + seqScanCount * 2;
  const complexityLabel =
    complexityScore < 20 ? "Low" : complexityScore <= 50 ? "Moderate" : "High";

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
    relationCount,
    complexityScore,
    complexityLabel,
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

function computeQueryHealth(summary, insights) {
  const highCount = insights.filter((insight) => insight.severity === "high").length;
  const mediumCount = insights.filter((insight) => insight.severity === "medium").length;
  const worstShare = summary?.worstTimeShare?.derived?.timePercent || 0;

  if (highCount > 0) {
    return { label: "Poor", score: 30 };
  }

  if (summary.hasSpill && worstShare > 0.3) {
    return { label: "Poor", score: 35 };
  }

  if (summary.avgEstimateError > 5) {
    return { label: "Moderate", score: 62 };
  }

  if (highCount === 0 && mediumCount === 0) {
    return { label: "Good", score: 90 };
  }

  return { label: "Moderate", score: 70 };
}

function groupIssuesForDisplay(insights) {
  const grouped = new Map();
  for (const insight of insights) {
    const key = [
      insight.severity || "",
      insight.category || "",
      insight.title || "",
      insight.explanation || "",
      insight.recommendation || "",
    ].join("::");

    if (!grouped.has(key)) {
      grouped.set(key, {
        severity: insight.severity,
        category: insight.category,
        title: insight.title,
        explanation: insight.explanation,
        recommendation: insight.recommendation,
        affectedNodes: [],
      });
    }

    grouped.get(key).affectedNodes.push({
      nodeId: insight.nodeId,
      nodeType: insight.nodeType,
      timePercent: insight.timePercent || 0,
    });
  }

  return Array.from(grouped.values()).sort((a, b) => {
    const severityDiff = (severityRank[b.severity] || 0) - (severityRank[a.severity] || 0);
    if (severityDiff !== 0) return severityDiff;
    const peakA = a.affectedNodes.reduce((max, node) => Math.max(max, node.timePercent || 0), 0);
    const peakB = b.affectedNodes.reduce((max, node) => Math.max(max, node.timePercent || 0), 0);
    return peakB - peakA;
  });
}

function generatePlanNarrative(root, summary, insights) {
  const parts = [];
  const bottleneck = summary.worstTimeShare;
  const mismatch = summary.worstMismatch;
  const highMismatch =
    mismatch && Number.isFinite(mismatch.score) && mismatch.score > 10;
  const hasNestedLoopExplosion = summary.maxJoinAmplification > 10;
  const majorIssueCount = insights.filter((insight) => insight.severity === "high" || insight.severity === "medium").length;

  parts.push(`This query scanned ${summary.relationCount} relation${summary.relationCount === 1 ? "" : "s"}.`);

  if (bottleneck) {
    parts.push(
      `Most time (${formatNumber(bottleneck.derived.timePercent * 100)}%) was spent in ${bottleneck.nodeType}.`,
    );
  }

  if (highMismatch && mismatch?.node) {
    parts.push(
      `A severe row estimate mismatch (${formatNumber(mismatch.score)}x) was observed in ${mismatch.node.nodeType}.`,
    );
  }

  if (summary.hasSpill) {
    parts.push("A spill to disk occurred, increasing I/O pressure.");
  }

  if (hasNestedLoopExplosion) {
    parts.push(`Join amplification reached ${formatNumber(summary.maxJoinAmplification)}x, indicating potential join explosion.`);
  }

  parts.push(`${majorIssueCount} major issue${majorIssueCount === 1 ? "" : "s"} were flagged for optimization.`);

  if (parts.length === 0) {
    return "Execution plan completed with no major bottlenecks detected.";
  }

  return parts.join(" ");
}

function getOptimizationPriorityNode(root) {
  const nodes = flattenTree(root);
  let best = null;
  let bestScore = -1;

  for (const node of nodes) {
    const rowEstimateError = getFoldedEstimateErrorFromRatio(node.derived.rowEstimateRatio);
    const score =
      (node.derived.timePercent || 0) *
      Math.log10(rowEstimateError + 1) *
      (node.derived.diskSpillDetected ? 1.5 : 1);

    if (score > bestScore) {
      bestScore = score;
      best = node;
    }
  }

  return best;
}

function App() {
  const [input, setInput] = useState("");
  const [beginnerMode, setBeginnerMode] = useState(true);
  const [tree, setTree] = useState(null);
  const [insights, setInsights] = useState([]);
  const [parseError, setParseError] = useState("");
  const [useSelfTime, setUseSelfTime] = useState(false);
  const [highlightMismatch, setHighlightMismatch] = useState(true);
  const [showEstVsActual, setShowEstVsActual] = useState(true);
  const [treeRenderSeed, setTreeRenderSeed] = useState(0);
  const [defaultOpen, setDefaultOpen] = useState(true);
  const [showAllBeginnerIssues, setShowAllBeginnerIssues] = useState(false);
  const [expandedIssueId, setExpandedIssueId] = useState(null);
  const [inputCollapsed, setInputCollapsed] = useState(false);
  const [executionTreeCollapsed, setExecutionTreeCollapsed] = useState(false);
  const [topIssuesCollapsed, setTopIssuesCollapsed] = useState(false);
  const [nodeTypeStatsCollapsed, setNodeTypeStatsCollapsed] = useState(false);
  const [relationStatsCollapsed, setRelationStatsCollapsed] = useState(false);
  const statsRef = useRef(null);
  const executionTreeRef = useRef(null);
  const nodeRefs = useRef(new Map());

  const summary = useMemo(() => (tree ? buildSummary(tree) : null), [tree]);
  const queryHealth = useMemo(
    () => (summary ? computeQueryHealth(summary, insights) : null),
    [summary, insights],
  );
  const groupedIssues = useMemo(() => groupIssuesForDisplay(insights), [insights]);
  const visibleIssueGroups = useMemo(() => {
    if (!beginnerMode) return groupedIssues;
    return showAllBeginnerIssues ? groupedIssues : groupedIssues.slice(0, 3);
  }, [groupedIssues, beginnerMode, showAllBeginnerIssues]);
  const planNarrative = useMemo(
    () => (tree && summary ? generatePlanNarrative(tree, summary, insights) : ""),
    [tree, summary, insights],
  );
  const fixFirstNode = useMemo(
    () => (tree ? getOptimizationPriorityNode(tree) : null),
    [tree],
  );
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

  function registerNodeRef(nodeId, element) {
    if (!nodeId) return;
    if (!element) {
      nodeRefs.current.delete(nodeId);
      return;
    }
    nodeRefs.current.set(nodeId, element);
  }

  function scrollToNode(nodeId) {
    const element = nodeRefs.current.get(nodeId);
    if (!element) return;
    element.scrollIntoView({ behavior: "smooth", block: "center" });
    element.classList.remove("node-flash");
    // Force reflow so repeated clicks retrigger animation.
    void element.offsetWidth;
    element.classList.add("node-flash");
    window.setTimeout(() => {
      element.classList.remove("node-flash");
    }, 2000);
  }

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
      setShowAllBeginnerIssues(false);
      setExpandedIssueId(null);
      setInputCollapsed(true);
      setTimeout(() => {
        if (beginnerMode) {
          statsRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
        } else {
          executionTreeRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
        }
      }, 100);
    } catch (error) {
      setTree(null);
      setInsights([]);
      setParseError(error.message || "Unable to parse plan.");
    }
  }

  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="header-row">
          <div className="brand-line">
            <span className="brand-banner">Query Lab</span>
            <label className="mode-toggle" title="Toggle between beginner and advanced analysis views.">
              <span className={`mode-label ${beginnerMode ? "active" : ""}`}>Basic</span>
              <input
                type="checkbox"
                checked={!beginnerMode}
                onChange={(event) => {
                  const advancedEnabled = event.target.checked;
                  setBeginnerMode(!advancedEnabled);
                  setShowAllBeginnerIssues(false);
                  setExpandedIssueId(null);
                }}
                aria-label="Toggle Beginner and Advanced mode"
              />
              <span className="mode-slider" />
              <span className={`mode-label ${!beginnerMode ? "active" : ""}`}>Advanced</span>
            </label>
            <span className="brand-tagline">PostgreSQL plan insights</span>
          </div>
        </div>
      </header>

      <section className="section-card first-accordion">
        <div
          className={`section-header ${!inputCollapsed ? "expanded" : ""}`}
          onClick={() => setInputCollapsed((value) => !value)}
          role="button"
          tabIndex={0}
          onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === " ") {
              event.preventDefault();
              setInputCollapsed((value) => !value);
            }
          }}
        >
          <span>Plan Input</span>
          <span className={`chevron ${!inputCollapsed ? "rotated" : ""}`}>▸</span>
        </div>
        {!inputCollapsed && (
          <div className="section-body input-panel">
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
          </div>
        )}
      </section>

      {tree && summary && (
        <>
          {beginnerMode && queryHealth && (
            <section ref={statsRef} className="panel section-major">
              <div className="summary-grid">
                <SummaryCard
                  title="Query Health"
                  value={`${queryHealth.label} (${queryHealth.score}/100)`}
                  description="Overall health grade inferred from plan warnings and runtime behavior."
                />
                <SummaryCard
                  title="Plan Complexity"
                  value={`${summary.complexityLabel} (${summary.complexityScore})`}
                  description="Complexity score based on node count, join amplification, and sequential scans."
                />
              </div>
              <div style={{ marginTop: 10, color: "var(--text-muted)", fontSize: 13 }}>{planNarrative}</div>
              <div className="summary-grid" style={{ marginTop: 10 }}>
                <SummaryCard
                  title="Total Execution Time"
                  value={`${formatNumber(summary.totalExecutionTime)} ms`}
                  description="Total inclusive runtime for the entire plan tree."
                />
                <SummaryCard
                  title="Top Bottleneck Operator"
                  value={
                    summary.worstTimeShare
                      ? `${summary.worstTimeShare.nodeType} (${formatNumber(summary.worstTimeShare.derived.timePercent * 100)}%)`
                      : "N/A"
                  }
                  description="Operator consuming the highest share of total runtime."
                />
                <SummaryCard
                  title="Largest Estimate Mismatch"
                  value={
                    summary.worstMismatch && summary.worstMismatch.node.derived.timePercent > 0.1
                      ? `${summary.worstMismatch.node.nodeType} (${formatNumber(summary.worstMismatch.score)}x)`
                      : "No major mismatch"
                  }
                  description="Highest row estimate mismatch among impactful nodes (>10% time share)."
                />
                <SummaryCard
                  title="Disk Spill Indicator"
                  value={summary.hasSpill ? "Spill Detected" : "No Spill"}
                  description="Indicates whether any operator spilled to disk."
                />
              </div>
            </section>
          )}

          <section ref={executionTreeRef} className="section-card section-major">
            <div
              className={`section-header ${!executionTreeCollapsed ? "expanded" : ""}`}
              onClick={() => setExecutionTreeCollapsed((value) => !value)}
              role="button"
              tabIndex={0}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  setExecutionTreeCollapsed((value) => !value);
                }
              }}
            >
              <span>Execution Tree</span>
              <span className={`chevron ${!executionTreeCollapsed ? "rotated" : ""}`}>▸</span>
            </div>
            {!executionTreeCollapsed && (
              <div className="section-body">
                <div className="execution-header">
                  <div className="execution-actions tree-actions">
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
                  beginnerMode={beginnerMode}
                  useSelfTime={useSelfTime}
                  highlightMismatch={highlightMismatch}
                  showEstVsActual={showEstVsActual}
                  insightsByNode={insightsByNode}
                  fixFirstNodeId={fixFirstNode?.id || null}
                  registerNodeRef={registerNodeRef}
                  renderSeed={treeRenderSeed}
                />
              </div>
            )}
          </section>

          <section className="section-card section-major">
            <div
              className={`section-header ${!topIssuesCollapsed ? "expanded" : ""}`}
              onClick={() => setTopIssuesCollapsed((value) => !value)}
              role="button"
              tabIndex={0}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  setTopIssuesCollapsed((value) => !value);
                }
              }}
            >
              <span>Top Issues in This Plan</span>
              <span className={`chevron ${!topIssuesCollapsed ? "rotated" : ""}`}>▸</span>
            </div>
            {!topIssuesCollapsed && (
              <div className="section-body">
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    gap: 12,
                  }}
                >
                  {beginnerMode && groupedIssues.length > 3 && (
                    <button
                      type="button"
                      className="node-action-btn show-issues-btn"
                      onClick={() => setShowAllBeginnerIssues((value) => !value)}
                    >
                      {showAllBeginnerIssues ? "Show Top 3" : `Show All Issues (${groupedIssues.length})`}
                    </button>
                  )}
                </div>
                <div className="insight-list">
              {visibleIssueGroups.map((group) => {
                const peakTimePercent = group.affectedNodes.reduce(
                  (max, node) => Math.max(max, node.timePercent || 0),
                  0,
                );
                const issueKey = `${group.severity}-${group.category}-${group.title}-${group.explanation}-${group.recommendation}`;
                const isExpanded = expandedIssueId === issueKey;
                const shortExplanation = `${String(group.explanation || "").split(".")[0]}.`;
                return (
                      <article key={issueKey} className={`issue-card severity-${group.severity}`}>
                        <div className="issue-header">
                          <div className="issue-header-main">
                            <span className={`badge badge-${group.severity}`}>{group.severity.toUpperCase()}</span>
                            <h3>{group.title}</h3>
                          </div>
                          <div className="issue-meta">
                            {formatNumber(peakTimePercent * 100)}% impact • {group.affectedNodes.length} nodes
                          </div>
                        </div>
                        <p className="issue-summary">{isExpanded ? group.explanation : shortExplanation}</p>
                        <button
                          type="button"
                          className="insight-expand-btn"
                      onClick={() => setExpandedIssueId((value) => (value === issueKey ? null : issueKey))}
                        >
                          {isExpanded ? "Hide details" : "View details"}
                        </button>
                        {isExpanded && (
                          <div className="issue-expanded">
                            <div className="issue-recommendation">
                              <p className="insight-recommendation">
                                <strong>Recommendation:</strong> {group.recommendation}
                              </p>
                            </div>
                            <div className="insight-affected-title">Affected nodes</div>
                            <div className="issue-node-chips">
                              {group.affectedNodes.map((node, index) => (
                                <button
                              key={`${group.title}-${node.nodeId}-${index}`}
                              type="button"
                              className="node-pill"
                              onClick={() => scrollToNode(node.nodeId)}
                            >
                              {node.nodeType} ({formatNumber((node.timePercent || 0) * 100)}%)
                            </button>
                          ))}
                        </div>
                      </div>
                    )}
                  </article>
                );
              })}
              {visibleIssueGroups.length === 0 && <div className="empty">No major issues detected.</div>}
                </div>
              </div>
            )}
          </section>

          {!beginnerMode && (
            <>
              <section ref={statsRef} className="panel summary-grid section-major">
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

              <section className="section-card section-major">
                <div
                  className={`section-header ${!nodeTypeStatsCollapsed ? "expanded" : ""}`}
                  onClick={() => setNodeTypeStatsCollapsed((value) => !value)}
                  role="button"
                  tabIndex={0}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      setNodeTypeStatsCollapsed((value) => !value);
                    }
                  }}
                >
                  <span>Statistics by Node Type</span>
                  <span className={`chevron ${!nodeTypeStatsCollapsed ? "rotated" : ""}`}>▸</span>
                </div>
                {!nodeTypeStatsCollapsed && (
                  <div className="section-body panel-table">
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
                        {nodeTypeStats.map((row) => {
                          const percent = (row.selfTime / Math.max(1, summary.totalExecutionTime)) * 100;
                          return (
                            <tr key={row.nodeType}>
                              <td>{row.nodeType}</td>
                              <td>{row.count}</td>
                              <td>{formatMs(row.selfTime)}</td>
                              <td title="Can exceed 100% due to loop amplification and cumulative work.">
                                {formatNumber(percent)}%
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </section>

              <section className="section-card section-major">
                <div
                  className={`section-header ${!relationStatsCollapsed ? "expanded" : ""}`}
                  onClick={() => setRelationStatsCollapsed((value) => !value)}
                  role="button"
                  tabIndex={0}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      setRelationStatsCollapsed((value) => !value);
                    }
                  }}
                >
                  <span>Statistics by Relation</span>
                  <span className={`chevron ${!relationStatsCollapsed ? "rotated" : ""}`}>▸</span>
                </div>
                {!relationStatsCollapsed && (
                  <div className="section-body panel-table">
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
                  </div>
                )}
              </section>
            </>
          )}
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
  beginnerMode,
  useSelfTime,
  highlightMismatch,
  showEstVsActual,
  insightsByNode,
  fixFirstNodeId,
  registerNodeRef,
  renderSeed,
}) {
  const [open, setOpen] = useState(defaultOpen);
  const [showInsights, setShowInsights] = useState(false);
  const [showDetails, setShowDetails] = useState(false);
  const [expandedNodeInsightId, setExpandedNodeInsightId] = useState(null);

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
  const beginnerEstimateError = getFoldedEstimateErrorFromRatio(rowRatio);
  const simplifiedBeginnerView = beginnerMode && !showDetails;

  const tooltip =
    `actual rows: ${formatNumber(node.actual.rows, 0)}\n` +
    `estimated rows: ${formatNumber(node.estimatedRows, 0)}\n` +
    `loops: ${formatNumber(node.actual.loops, 0)}\n` +
    `buffers shared hit/read: ${formatNumber(node.buffers.sharedHit, 0)}/${formatNumber(node.buffers.sharedRead, 0)}\n` +
    `buffers temp read/written: ${formatNumber(node.buffers.tempRead, 0)}/${formatNumber(node.buffers.tempWritten, 0)}\n` +
    `effective time: ${formatNumber(node.derived.effectiveTime)} ms`;

  return (
    <div className="tree-node" ref={(el) => registerNodeRef(node.id, el)}>
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
            {node.id === fixFirstNodeId && (
              <span className="badge badge-medium" title="This node has the highest optimization impact score.">
                Highest Optimization Impact
              </span>
            )}
            {severity && <span className={`badge badge-${severity}`}>{severity.toUpperCase()}</span>}
            <button
              type="button"
              onClick={() => setShowDetails((value) => !value)}
              className="node-action-btn"
              title="Show SQL-level node details such as filters, join/sort keys, and referenced columns."
            >
              {showDetails ? "Hide Details" : "Details"}
            </button>
            {nodeInsights.length > 0 && (!beginnerMode || showDetails) && (
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
          {simplifiedBeginnerView ? (
            <div className="node-metrics">
              time {formatNumber(node.derived.timePercent * 100)}% | estimate error {formatNumber(beginnerEstimateError)}x
              {node.derived.diskSpillDetected ? " | spill detected" : ""}
            </div>
          ) : showEstVsActual ? (
            <div className="node-metrics">
              est rows {formatNumber(node.estimatedRows, 0)} | actual rows {formatNumber(node.actual.rows, 0)} | loops{" "}
              {formatNumber(node.actual.loops, 0)} | row ratio {formatNumber(rowRatio)}
            </div>
          ) : null}
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

      {showInsights && nodeInsights.length > 0 && (!beginnerMode || showDetails) && (
        <div className="insight-list node-insights-list">
          {nodeInsights.map((insight, index) => (
            <article key={`${insight.nodeId}-${index}`} className={`node-insight-card severity-${insight.severity}`}>
              <div className="node-insight-header">
                <div className="node-insight-title">
                  <span className={`badge badge-${insight.severity}`}>{insight.severity.toUpperCase()}</span>
                  <h3>{insight.title}</h3>
                </div>
                <div className="node-insight-meta">
                  {formatNumber((insight.timePercent || 0) * 100)}% impact
                </div>
              </div>
              <p className="node-insight-summary" title="What this means for the execution behavior of this node.">
                {expandedNodeInsightId === index
                  ? insight.explanation
                  : `${String(insight.explanation || "").split(".")[0]}.`}
              </p>
              <button
                type="button"
                className="insight-expand-btn"
                onClick={() => setExpandedNodeInsightId((value) => (value === index ? null : index))}
              >
                {expandedNodeInsightId === index ? "Hide details" : "View details"}
              </button>
              {expandedNodeInsightId === index && (
                <div className="node-insight-expanded">
                  <p className="node-insight-recommendation">
                    <strong title="Actionable SQL or indexing step to improve this node.">Recommendation:</strong>{" "}
                    {insight.recommendation}
                  </p>
                </div>
              )}
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
              beginnerMode={beginnerMode}
              fixFirstNodeId={fixFirstNodeId}
              registerNodeRef={registerNodeRef}
              renderSeed={renderSeed}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export default App;
