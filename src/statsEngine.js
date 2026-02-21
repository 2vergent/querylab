export default function statsEngine(root) {
  function walk(node) {
    node.children.forEach(walk);

    const effectiveTime = node.actual.totalTime * node.actual.loops;
    const childEffectiveTime = node.children.reduce(
      (sum, child) => sum + (child.derived.effectiveTime || 0),
      0,
    );
    const selfTime = Math.max(0, effectiveTime - childEffectiveTime);

    const actualRows = node.actual.rows;
    const estimatedRows = node.estimatedRows;
    let rowEstimateRatio = 1;
    if (estimatedRows > 0) {
      rowEstimateRatio = actualRows / estimatedRows;
    } else if (actualRows > 0) {
      rowEstimateRatio = Number.POSITIVE_INFINITY;
    }

    const finiteRatio = Number.isFinite(rowEstimateRatio) ? rowEstimateRatio : Number.MAX_SAFE_INTEGER;
    const normalizedError = finiteRatio <= 0 ? Number.MAX_SAFE_INTEGER : Math.max(finiteRatio, 1 / finiteRatio);
    const log10RowEstimateError = Math.log10(Math.max(1, normalizedError));

    const parentRowsProcessed = actualRows * node.actual.loops;
    const childRowsProcessed = node.children.reduce(
      (sum, child) => sum + child.actual.rows * child.actual.loops,
      0,
    );
    const amplificationFactor =
      /join/i.test(node.nodeType) || node.nodeType === "Nested Loop"
        ? childRowsProcessed / Math.max(1, parentRowsProcessed)
        : null;

    const totalBuffersRead =
      node.buffers.sharedRead + node.buffers.localRead + node.buffers.tempRead;
    const diskSpillDetected =
      (node.metadata.sortSpaceType || "").toLowerCase() === "disk" ||
      node.metadata.sortMethod?.toLowerCase().includes("external") ||
      node.buffers.tempRead > 0 ||
      node.buffers.tempWritten > 0;

    const parallelEfficiencyRatio =
      node.workers.planned > 0
        ? Math.min(1, node.workers.launched / node.workers.planned)
        : 1;

    node.derived = {
      ...node.derived,
      effectiveTime,
      selfTime,
      rowEstimateRatio,
      log10RowEstimateError,
      amplificationFactor,
      totalBuffersRead,
      diskSpillDetected,
      parallelEfficiencyRatio,
    };
  }

  walk(root);

  const rootEffectiveTime = Math.max(1, root.derived.effectiveTime || 0);
  function applyPercent(node) {
    node.derived.timePercent = node.derived.effectiveTime / rootEffectiveTime;
    node.children.forEach(applyPercent);
  }
  applyPercent(root);
}
