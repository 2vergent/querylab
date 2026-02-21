function parseNumber(value) {
  if (value === undefined || value === null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseMaybeSizeKb(value) {
  if (!value) return null;
  const match = String(value).trim().match(/^([0-9]+(?:\.[0-9]+)?)\s*(kB|MB|GB)?$/i);
  if (!match) return null;
  const amount = Number(match[1]);
  const unit = (match[2] || "kB").toLowerCase();
  if (!Number.isFinite(amount)) return null;
  if (unit === "kb") return amount;
  if (unit === "mb") return amount * 1024;
  if (unit === "gb") return amount * 1024 * 1024;
  return amount;
}

function parseBuffers(line) {
  const clean = line.replace(/^Buffers:\s*/i, "");
  const result = {};
  const tokenRegex = /\b(?:(shared|local|temp)\s+)?(hit|read|written|dirtied)=([0-9]+)/gi;
  let currentScope = null;
  let match = null;

  while ((match = tokenRegex.exec(clean)) !== null) {
    const scope = (match[1] || currentScope || "").toLowerCase();
    if (!scope) continue;
    currentScope = scope;
    const kind = match[2].toLowerCase();
    const value = Number(match[3]);
    result[`${scope}${kind[0].toUpperCase()}${kind.slice(1)}`] = value;
  }

  return result;
}

function parseNodeLine(content) {
  const core = content.replace(/^->\s*/, "").trim();
  const costGroupMatch = core.match(/\(cost=[^)]+\)/i);
  const actualGroupMatch = core.match(/\(actual\s+time=[^)]+\)/i);
  const costMatch = costGroupMatch?.[0]?.match(/cost=\s*([0-9.]+)\.\.([0-9.]+)/i);
  const planRowsMatch = costGroupMatch?.[0]?.match(/rows=\s*([0-9]+)/i);
  const widthMatch = costGroupMatch?.[0]?.match(/width=\s*([0-9]+)/i);
  const actualMatch = actualGroupMatch?.[0]?.match(/actual\s+time=\s*([0-9.]+)\.\.([0-9.]+)/i);
  const actualRowsMatch = actualGroupMatch?.[0]?.match(/rows=\s*([0-9]+)/i);
  const loopsMatch = actualGroupMatch?.[0]?.match(/loops=\s*([0-9]+)/i);

  const descriptor = core.split(/\s+\(cost=/i)[0].trim();
  const usingOnMatch = descriptor.match(/^(?<nodeType>[A-Za-z][A-Za-z ]*?)\s+using\s+(?<indexName>\S+)\s+on\s+(?<relationName>\S+)/);
  const onMatch = descriptor.match(/^(?<nodeType>[A-Za-z][A-Za-z ]*?)\s+on\s+(?<relationName>\S+)/);
  const usingMatch = descriptor.match(/^(?<nodeType>[A-Za-z][A-Za-z ]*?)\s+using\s+(?<indexName>\S+)/);
  const plainTypeMatch = descriptor.match(/^(?<nodeType>[A-Za-z][A-Za-z ]*)$/);

  let nodeType = "Unknown";
  let relationName = null;
  let indexName = null;

  if (usingOnMatch?.groups) {
    nodeType = usingOnMatch.groups.nodeType.trim();
    relationName = usingOnMatch.groups.relationName.trim();
    indexName = usingOnMatch.groups.indexName.trim();
  } else if (onMatch?.groups) {
    nodeType = onMatch.groups.nodeType.trim();
    relationName = onMatch.groups.relationName.trim();
  } else if (usingMatch?.groups) {
    nodeType = usingMatch.groups.nodeType.trim();
    indexName = usingMatch.groups.indexName.trim();
  } else if (plainTypeMatch?.groups) {
    nodeType = plainTypeMatch.groups.nodeType.trim();
  }

  return {
    nodeType,
    relationName,
    indexName,
    costStartup: parseNumber(costMatch?.[1]),
    costTotal: parseNumber(costMatch?.[2]),
    planRows: parseNumber(planRowsMatch?.[1]),
    actualStartup: parseNumber(actualMatch?.[1]),
    actualTotal: parseNumber(actualMatch?.[2]),
    actualRows: parseNumber(actualRowsMatch?.[1]),
    actualLoops: parseNumber(loopsMatch?.[1]),
    width: parseNumber(widthMatch?.[1]),
  };
}

function normalizePgAdminText(text) {
  const raw = String(text || "").trim();
  if (!raw) return raw;

  const quotedBlocks = [...raw.matchAll(/"([^"]*)"/g)].map((match) => match[1]);
  let normalized = quotedBlocks.length > 1 ? quotedBlocks.join("\n") : raw;

  normalized = normalized
    .split(/\r?\n/)
    .map((line) => {
      let next = line.trimEnd();
      if (next.endsWith(",")) next = next.slice(0, -1);
      if (next.startsWith('"')) next = next.slice(1);
      if (next.endsWith('"')) next = next.slice(0, -1);
      return next;
    })
    .join("\n");

  return normalized;
}

function parseText(text) {
  const normalizedText = normalizePgAdminText(text);
  const lines = normalizedText.split(/\r?\n/);
  const stack = [];
  const frameStack = [];
  let root = null;
  let lastNode = null;
  let inPlanningSection = false;

  const arrowIndents = lines
    .filter((line) => /^\s*->/.test(line || ""))
    .map((line) => line.indexOf("->"))
    .filter((value) => value >= 0);
  const flattenedArrowLayout = arrowIndents.length >= 3 && new Set(arrowIndents).size === 1;

  const expectedChildren = (nodeType) => {
    if (!nodeType) return 1;
    if (/Join/i.test(nodeType) || nodeType === "Nested Loop") return 2;
    if (nodeType === "Bitmap Heap Scan") return 1;
    if (nodeType === "Hash") return 1;
    if (nodeType === "Sort") return 1;
    if (nodeType === "Gather" || nodeType === "Gather Merge") return 1;
    if (nodeType === "GroupAggregate" || nodeType === "Aggregate") return 1;
    if (nodeType === "Memoize" || nodeType === "Materialize") return 1;
    if (/Scan$/i.test(nodeType)) return 0;
    return 1;
  };

  for (const rawLine of lines) {
    if (!rawLine || !rawLine.trim()) continue;

    const indent = rawLine.search(/\S/);
    const content = rawLine.trim();
    const workerNormalizedContent = content.replace(/^Worker\s+\d+:\s*/i, "");

    if (/^Planning:/i.test(workerNormalizedContent)) {
      inPlanningSection = true;
      continue;
    }

    if (/^(Planning Time|Execution Time):/i.test(workerNormalizedContent)) {
      if (/^Execution Time:/i.test(workerNormalizedContent)) {
        inPlanningSection = false;
      }
      continue;
    }

    if (inPlanningSection) continue;

    const isNodeLine =
      /^(->\s*)?[A-Za-z][A-Za-z ]*(\s+using\s+\S+)?(\s+on\s+\S+)?\s+\(cost=/i.test(workerNormalizedContent) ||
      /^(->\s*)?[A-Za-z][A-Za-z ]*(\s+using\s+\S+)?(\s+on\s+\S+)?\s+\(actual\s+time=/i.test(workerNormalizedContent);

    if (isNodeLine) {
      const parsed = parseNodeLine(workerNormalizedContent);
      const node = {
        "Node Type": parsed.nodeType,
        "Relation Name": parsed.relationName,
        "Index Name": parsed.indexName,
        "Startup Cost": parsed.costStartup,
        "Total Cost": parsed.costTotal,
        "Plan Rows": parsed.planRows,
        "Plan Width": parsed.width,
        "Actual Startup Time": parsed.actualStartup,
        "Actual Total Time": parsed.actualTotal,
        "Actual Rows": parsed.actualRows,
        "Actual Loops": parsed.actualLoops,
        children: [],
        _indent: indent,
      };

      if (!flattenedArrowLayout) {
        while (stack.length && stack[stack.length - 1]._indent >= indent) {
          stack.pop();
        }

        if (stack.length === 0) {
          root = node;
        } else {
          stack[stack.length - 1].children.push(node);
        }
        stack.push(node);
      } else {
        if (!root) {
          root = node;
          const children = expectedChildren(node["Node Type"]);
          if (children > 0) frameStack.push({ node, remaining: children });
        } else {
          while (frameStack.length > 0 && frameStack[frameStack.length - 1].remaining <= 0) {
            frameStack.pop();
          }
          const parentFrame = frameStack[frameStack.length - 1];
          const parent = parentFrame?.node || root;
          parent.children.push(node);
          if (parentFrame && parentFrame.remaining > 0) {
            parentFrame.remaining -= 1;
          }

          const children = expectedChildren(node["Node Type"]);
          if (children > 0) frameStack.push({ node, remaining: children });
        }
      }

      lastNode = node;
      continue;
    }

    if (!lastNode) continue;

    if (/^Buffers:/i.test(workerNormalizedContent)) {
      const parsed = parseBuffers(workerNormalizedContent);
      for (const [k, v] of Object.entries(parsed)) {
        if (k === "sharedHit") lastNode["Shared Hit Blocks"] = v;
        if (k === "sharedRead") lastNode["Shared Read Blocks"] = v;
        if (k === "sharedWritten") lastNode["Shared Written Blocks"] = v;
        if (k === "sharedDirtied") lastNode["Shared Dirtied Blocks"] = v;
        if (k === "localHit") lastNode["Local Hit Blocks"] = v;
        if (k === "localRead") lastNode["Local Read Blocks"] = v;
        if (k === "localWritten") lastNode["Local Written Blocks"] = v;
        if (k === "localDirtied") lastNode["Local Dirtied Blocks"] = v;
        if (k === "tempRead") lastNode["Temp Read Blocks"] = v;
        if (k === "tempWritten") lastNode["Temp Written Blocks"] = v;
        if (k === "tempDirtied") lastNode["Temp Dirtied Blocks"] = v;
      }
      continue;
    }

    const conditionPatterns = [
      [/^Filter:\s*(.+)$/i, "Filter"],
      [/^Hash Cond:\s*(.+)$/i, "Hash Cond"],
      [/^Join Filter:\s*(.+)$/i, "Join Filter"],
      [/^Recheck Cond:\s*(.+)$/i, "Recheck Cond"],
      [/^Index Cond:\s*(.+)$/i, "Index Cond"],
    ];

    let matchedCondition = false;
    for (const [pattern, key] of conditionPatterns) {
      const match = workerNormalizedContent.match(pattern);
      if (match) {
        lastNode[key] = match[1].trim();
        matchedCondition = true;
        break;
      }
    }
    if (matchedCondition) continue;

    const workersMatch = workerNormalizedContent.match(/^Workers\s+(Planned|Launched):\s*([0-9]+)/i);
    if (workersMatch) {
      const kind = workersMatch[1].toLowerCase();
      const value = Number(workersMatch[2]);
      if (kind === "planned") lastNode["Workers Planned"] = value;
      if (kind === "launched") lastNode["Workers Launched"] = value;
      continue;
    }

    const sortMethodMatch = workerNormalizedContent.match(
      /^Sort Method:\s*([A-Za-z ]+?)(?:\s+Memory:\s*([0-9.]+\s*(?:kB|MB|GB)?))?(?:\s+Disk:\s*([0-9.]+\s*(?:kB|MB|GB)?))?$/i,
    );
    if (sortMethodMatch) {
      lastNode["Sort Method"] = sortMethodMatch[1].trim();
      if (sortMethodMatch[2]) {
        lastNode["Sort Space Used"] = parseMaybeSizeKb(sortMethodMatch[2]);
        lastNode["Sort Space Type"] = "Memory";
      }
      if (sortMethodMatch[3]) {
        lastNode["Sort Space Used"] = parseMaybeSizeKb(sortMethodMatch[3]);
        lastNode["Sort Space Type"] = "Disk";
      }
      continue;
    }

    const heapFetchesMatch = workerNormalizedContent.match(/^Heap Fetches:\s*([0-9]+)/i);
    if (heapFetchesMatch) {
      lastNode["Heap Fetches"] = Number(heapFetchesMatch[1]);
      continue;
    }

    const rowsRemovedMatch = workerNormalizedContent.match(/^Rows Removed by Filter:\s*([0-9]+)/i);
    if (rowsRemovedMatch) {
      lastNode["Rows Removed by Filter"] = Number(rowsRemovedMatch[1]);
      continue;
    }

    const keyValueMatch = workerNormalizedContent.match(/^([A-Za-z][A-Za-z ]+):\s*(.+)$/);
    if (keyValueMatch) {
      const key = keyValueMatch[1].trim();
      const value = keyValueMatch[2].trim();
      if (key && value) {
        lastNode[key] = value;
      }
    }
  }

  const stripIndent = (node) => {
    if (!node) return null;
    const { _indent, children, ...rest } = node;
    return {
      ...rest,
      children: (children || []).map(stripIndent),
    };
  };

  return stripIndent(root);
}

function findPlanRoot(json) {
  if (Array.isArray(json) && json[0]?.Plan) return json[0].Plan;
  if (json?.Plan) return json.Plan;
  return json;
}

export default function parseInput(input) {
  try {
    const json = JSON.parse(input);
    const root = findPlanRoot(json);
    return root || null;
  } catch {
    return parseText(input);
  }
}
