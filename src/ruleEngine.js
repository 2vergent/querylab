export default function ruleEngine(tree) {
  const insights = [];

  const addInsight = (node, payload) => {
    insights.push({
      ...payload,
      nodeId: node.id,
    });
  };

  const rules = [
    function sequentialScanLargeDataset(node) {
      if (node.nodeType !== "Seq Scan") return;
      if (node.actual.rows < 50000 && node.derived.totalBuffersRead < 10000) return;
      addInsight(node, {
        severity: "high",
        category: "Scan",
        title: "Sequential Scan on large dataset",
        explanation:
          "This node performed a full table scan with high row volume or buffer reads, indicating broad page access.",
        recommendation:
          "Add or tune selective indexes, rewrite predicates for index usability, and confirm statistics are current.",
      });
    },
    function nestedLoopAmplification(node) {
      if (node.nodeType !== "Nested Loop") return;
      if ((node.derived.amplificationFactor || 0) <= 10) return;
      addInsight(node, {
        severity: "high",
        category: "Join",
        title: "Nested Loop amplification explosion",
        explanation:
          "Nested Loop consumed far more child rows than it produced, indicating repeated inner scans per outer row.",
        recommendation:
          "Add join indexes on the inner side, evaluate Hash/Merge join alternatives, and reduce outer row count early.",
      });
    },
    function estimateMismatch(node) {
      const ratio = node.derived.rowEstimateRatio;
      if (!Number.isFinite(ratio) || ratio <= 0) {
        addInsight(node, {
          severity: "high",
          category: "Cardinality",
          title: "Severe row estimate mismatch (>10x)",
          explanation:
            "Planner estimated zero or near-zero rows while execution returned rows, showing a major cardinality model error.",
          recommendation:
            "Run ANALYZE, increase statistics targets on skewed columns, and validate predicate correlation assumptions.",
        });
        return;
      }

      const folded = Math.max(ratio, 1 / ratio);
      if (folded > 10) {
        addInsight(node, {
          severity: "high",
          category: "Cardinality",
          title: "Severe row estimate mismatch (>10x)",
          explanation:
            "Actual row count diverged from estimate by more than 10x, which can cause poor join order and operator choices.",
          recommendation:
            "Refresh statistics, inspect data skew and correlation, and consider extended statistics on correlated predicates.",
        });
      } else if (folded > 3) {
        addInsight(node, {
          severity: "medium",
          category: "Cardinality",
          title: "Row estimate mismatch (>3x)",
          explanation:
            "Row estimates materially differ from observed rows, reducing planner reliability for this subtree.",
          recommendation:
            "Analyze affected tables and review expression/functional predicates that may limit selectivity estimation.",
        });
      }
    },
    function sortSpill(node) {
      if (node.nodeType !== "Sort") return;
      if (!node.derived.diskSpillDetected) return;
      addInsight(node, {
        severity: "high",
        category: "Memory",
        title: "Sort spilled to disk",
        explanation:
          "Sort used external storage (disk or temp blocks), adding I/O overhead and increasing latency.",
        recommendation:
          "Increase work_mem carefully, reduce sort input rows/width earlier, or create indexes that satisfy sort order.",
      });
    },
    function highSelfTime(node) {
      if (node.derived.selfTime < 50) return;
      if (node.derived.timePercent < 0.2) return;
      addInsight(node, {
        severity: "medium",
        category: "Execution",
        title: "High self time node",
        explanation:
          "A large share of execution time is spent in this operator itself, not in its children.",
        recommendation:
          "Focus tuning directly on this operator's access path, predicate efficiency, or memory behavior.",
      });
    },
    function excessiveBufferReads(node) {
      if (node.derived.totalBuffersRead < 10000) return;
      addInsight(node, {
        severity: "high",
        category: "I/O",
        title: "Excessive buffer reads",
        explanation:
          "This node triggered substantial disk-oriented buffer reads, indicating I/O-heavy execution.",
        recommendation:
          "Improve selectivity with indexes, reduce scanned pages, and evaluate cache hit rate and storage throughput.",
      });
    },
    function missingParallelism(node) {
      const supportsParallelOpportunity =
        /Seq Scan|Hash Join|Aggregate|Gather Merge|Sort/i.test(node.nodeType) &&
        node.actual.rows > 100000 &&
        node.derived.effectiveTime > 200;
      if (!supportsParallelOpportunity) return;
      if (node.workers.planned > 0) return;
      addInsight(node, {
        severity: "medium",
        category: "Parallelism",
        title: "Missing parallelism opportunity",
        explanation:
          "Large and expensive operator executed without planned workers, leaving potential CPU parallelism unused.",
        recommendation:
          "Review parallel settings (`max_parallel_workers_per_gather`, cost thresholds) and function parallel-safety.",
      });
    },
    function bitmapRecheckInefficiency(node) {
      if (node.nodeType !== "Bitmap Heap Scan") return;
      const heapFetches = node.metadata.heapFetches;
      if (heapFetches <= 0) return;
      if (!node.metadata.recheckCond) return;
      const ratio = heapFetches / Math.max(1, node.actual.rows);
      if (ratio <= 1.5) return;
      addInsight(node, {
        severity: "medium",
        category: "Scan",
        title: "Bitmap recheck inefficiency",
        explanation:
          "Bitmap heap scan performed many heap fetches relative to output rows, indicating expensive rechecks.",
        recommendation:
          "Use more selective indexes, reduce lossy bitmap pages, and tighten predicates to reduce heap rechecks.",
      });
    },
    function hashBuildLargerThanEstimate(node) {
      if (node.nodeType !== "Hash") return;
      const ratio = node.derived.rowEstimateRatio;
      if (Number.isFinite(ratio) && ratio <= 3) return;
      addInsight(node, {
        severity: "high",
        category: "Join",
        title: "Hash build larger than estimate",
        explanation:
          "Hash build side produced significantly more rows than expected, increasing memory pressure and join cost.",
        recommendation:
          "Refresh statistics for hash build relations and reduce build input cardinality before the hash operator.",
      });
    },
  ];

  function walk(node) {
    rules.forEach((rule) => rule(node));
    node.children.forEach(walk);
  }

  walk(tree);
  return insights;
}
