/* Interactive code map for valkey-ci-agent.
 *
 * Reads graph.json (produced by `python -m scripts.codemap build`) and renders
 * two linked views: a module-level overview and a function-level call graph
 * rooted at whatever you are walking through.
 */

'use strict';

const REPO_BLOB = 'https://github.com/valkey-io/valkey-ci-agent/blob/main';

const CONFIDENCE_COLOR = {
  exact: '#6cc4a1',
  inferred: '#e4c86b',
  ambiguous: '#f2775e',
};

// A leaf utility called from everywhere (retry_github_call has 50 callers) acts
// as a gravity well: every caller drags an edge across the whole layout to the
// same box. Collapsing those into a badge on the caller halves edge crossings
// without hiding the fact that the call happens.
const HUB_MIN_CALLERS = 8;
const HUB_MAX_CALLEES = 2;

// Layering release_notes.main at depth 4 produced ranks of 1,6,7,22,36,11,6,3.
// A 36-node rank is unreadable at any zoom, and capping each parent's fan-out
// does not help because several wide parents stack into the same rank -- so the
// cap has to apply to the rank itself.
const MAX_RANK_WIDTH = 12;

const state = {
  view: 'functions',
  root: null,
  depth: 2,
  direction: 'callees',
  hidePrivate: false,
  hideClasses: false,
  confidences: new Set(['exact', 'inferred', 'ambiguous']),
  disabledPackages: new Set(),
  expanded: new Set(),
  trail: [],
  selected: null,
};

/** @type {{nodes: Map<string, object>, modules: Map<string, object>,
 *          out: Map<string, object[]>, in: Map<string, object[]>,
 *          moduleOut: Map<string, object[]>, moduleIn: Map<string, object[]>,
 *          meta: object, packageColor: Map<string, string>}} */
const index = {
  nodes: new Map(),
  modules: new Map(),
  out: new Map(),
  in: new Map(),
  moduleOut: new Map(),
  moduleIn: new Map(),
  collapsible: new Set(),
  meta: null,
  packageColor: new Map(),
};

let cy = null;
let currentOverflow = new Map();

/* ------------------------------------------------------------------ utils */

function el(id) {
  return document.getElementById(id);
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[ch]));
}

function push(map, key, value) {
  const bucket = map.get(key);
  if (bucket) bucket.push(value);
  else map.set(key, [value]);
}

function shortName(id) {
  const colon = id.indexOf(':');
  return colon === -1 ? id : id.slice(colon + 1);
}

function moduleOf(id) {
  const colon = id.indexOf(':');
  return colon === -1 ? id : id.slice(0, colon);
}

function shortModule(moduleName) {
  return moduleName.replace(/^scripts\./, '');
}

function packageColor(name) {
  return index.packageColor.get(name) || '#8b95a5';
}

/* ------------------------------------------------------------------ load */

async function load() {
  let graph;
  try {
    const response = await fetch('graph.json', { cache: 'no-cache' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    graph = await response.json();
  } catch (error) {
    el('cy').innerHTML =
      `<div class="error">Could not load graph.json (${escapeHtml(error.message)}).<br><br>` +
      `Serve this directory over HTTP:<br><code>python -m scripts.codemap serve</code></div>`;
    return;
  }

  index.meta = graph.meta;
  graph.meta.packages.forEach((pkg) => index.packageColor.set(pkg.name, pkg.color));
  graph.nodes.forEach((node) => index.nodes.set(node.id, node));
  graph.modules.forEach((module) => index.modules.set(module.id, module));

  graph.edges.forEach((edge) => {
    push(index.out, edge.source, edge);
    push(index.in, edge.target, edge);
  });
  graph.moduleEdges.forEach((edge) => {
    push(index.moduleOut, edge.source, edge);
    push(index.moduleIn, edge.target, edge);
  });

  index.nodes.forEach((node, id) => {
    const callers = (index.in.get(id) || []).length;
    const callees = (index.out.get(id) || []).length;
    if (callers >= HUB_MIN_CALLERS && callees <= HUB_MAX_CALLEES) {
      index.collapsible.add(id);
    }
  });

  const meta = graph.meta;
  el('brand-stats').textContent =
    `${meta.moduleCount} modules · ${meta.nodeCount} symbols · ` +
    `${meta.edgeCount} calls · ${meta.commit}`;

  buildLegend();
  buildEntrypoints();
  wireControls();
  initCytoscape();

  if (!restoreFromHash()) {
    const first = entrypointNodes()[0];
    setRoot(first ? first.id : null, { reset: true });
  }
}

/* ------------------------------------------------------------- side panels */

function entrypointNodes() {
  return [...index.nodes.values()]
    .filter((node) => node.entryWorkflows && node.entryWorkflows.length)
    .sort((a, b) => a.entryWorkflows[0].localeCompare(b.entryWorkflows[0]));
}

function buildEntrypoints() {
  const list = el('entrypoints');
  const rows = [];
  entrypointNodes().forEach((node) => {
    node.entryWorkflows.forEach((workflow) => {
      rows.push({ workflow, node });
    });
  });
  rows.sort((a, b) => a.workflow.localeCompare(b.workflow));

  list.innerHTML = rows.map(({ workflow, node }) => `
    <li data-node="${escapeHtml(node.id)}" title="${escapeHtml(node.id)}">
      <span class="wf">${escapeHtml(workflow)}</span>
      <span class="mod">${escapeHtml(shortModule(node.module))}</span>
    </li>`).join('');

  list.querySelectorAll('li').forEach((item) => {
    item.addEventListener('click', () => {
      state.view = 'functions';
      syncViewButtons();
      setRoot(item.dataset.node, { reset: true });
    });
  });
}

function buildLegend() {
  const counts = new Map();
  index.nodes.forEach((node) => {
    counts.set(node.package, (counts.get(node.package) || 0) + 1);
  });

  const list = el('legend');
  list.innerHTML = [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => `
      <li data-pkg="${escapeHtml(name)}" data-off="false">
        <span class="swatch" style="background:${packageColor(name)}"></span>
        <span>${escapeHtml(name)}</span>
        <span class="count">${count}</span>
      </li>`).join('');

  list.querySelectorAll('li').forEach((item) => {
    item.addEventListener('click', () => {
      const pkg = item.dataset.pkg;
      if (state.disabledPackages.has(pkg)) state.disabledPackages.delete(pkg);
      else state.disabledPackages.add(pkg);
      item.dataset.off = String(state.disabledPackages.has(pkg));
      render();
    });
  });
}

function syncViewButtons() {
  el('view-modules').setAttribute('aria-selected', String(state.view === 'modules'));
  el('view-functions').setAttribute('aria-selected', String(state.view === 'functions'));
  el('stagehint').textContent = state.view === 'modules'
    ? 'click a module to inspect · double-click to open its functions'
    : 'click a node to inspect · double-click to walk into it';
}

function wireControls() {
  el('view-modules').addEventListener('click', () => {
    state.view = 'modules';
    state.root = null;
    state.trail = [];
    syncViewButtons();
    render();
  });

  el('view-functions').addEventListener('click', () => {
    state.view = 'functions';
    syncViewButtons();
    if (!state.root) {
      const first = entrypointNodes()[0];
      setRoot(first ? first.id : null, { reset: true });
    } else {
      render();
    }
  });

  el('depth').addEventListener('input', (event) => {
    state.depth = Number(event.target.value);
    el('depth-out').textContent = state.depth;
    render();
  });

  el('direction').addEventListener('change', (event) => {
    state.direction = event.target.value;
    render();
  });

  el('hide-private').addEventListener('change', (event) => {
    state.hidePrivate = event.target.checked;
    render();
  });

  el('hide-classes').addEventListener('change', (event) => {
    state.hideClasses = event.target.checked;
    render();
  });

  document.querySelectorAll('[data-conf]').forEach((box) => {
    box.addEventListener('change', () => {
      if (box.checked) state.confidences.add(box.dataset.conf);
      else state.confidences.delete(box.dataset.conf);
      render();
    });
  });

  wireSearch();
  window.addEventListener('hashchange', restoreFromHash);
}

/* ---------------------------------------------------------------- search */

function wireSearch() {
  const input = el('search');
  const results = el('search-results');
  let matches = [];
  let cursor = -1;

  const close = () => {
    results.hidden = true;
    cursor = -1;
  };

  const commit = (i) => {
    const match = matches[i];
    if (!match) return;
    input.value = '';
    close();
    if (match.type === 'module') {
      state.view = 'modules';
      syncViewButtons();
      setRoot(match.id, { reset: true });
    } else {
      state.view = 'functions';
      syncViewButtons();
      setRoot(match.id, { reset: true });
    }
  };

  input.addEventListener('input', () => {
    const query = input.value.trim().toLowerCase();
    if (query.length < 2) return close();

    const scored = [];
    index.nodes.forEach((node) => {
      const name = node.name.toLowerCase();
      const full = node.id.toLowerCase();
      let score = -1;
      if (name === query) score = 0;
      else if (name.startsWith(query)) score = 1;
      else if (name.includes(query)) score = 2;
      else if (full.includes(query)) score = 3;
      if (score >= 0) scored.push({ score, id: node.id, type: 'symbol', node });
    });
    index.modules.forEach((module) => {
      if (module.id.toLowerCase().includes(query)) {
        scored.push({ score: 2.5, id: module.id, type: 'module', node: module });
      }
    });

    matches = scored
      .sort((a, b) => a.score - b.score || a.id.length - b.id.length)
      .slice(0, 40);

    if (!matches.length) {
      results.innerHTML = '<li aria-disabled="true"><span class="sr-name">no match</span></li>';
      results.hidden = false;
      return;
    }

    results.innerHTML = matches.map((match, i) => {
      const label = match.type === 'module'
        ? shortModule(match.id)
        : shortName(match.id);
      const where = match.type === 'module'
        ? 'module'
        : shortModule(moduleOf(match.id));
      return `<li data-i="${i}" aria-selected="false">
        <span class="sr-name">${escapeHtml(label)}</span>
        <span class="sr-mod">${escapeHtml(where)}</span>
      </li>`;
    }).join('');
    results.hidden = false;

    results.querySelectorAll('li[data-i]').forEach((item) => {
      item.addEventListener('mousedown', (event) => {
        event.preventDefault();
        commit(Number(item.dataset.i));
      });
    });
  });

  input.addEventListener('keydown', (event) => {
    if (results.hidden) return;
    const items = [...results.querySelectorAll('li[data-i]')];
    if (!items.length) return;

    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      if (cursor >= 0) items[cursor].setAttribute('aria-selected', 'false');
      cursor = event.key === 'ArrowDown'
        ? (cursor + 1) % items.length
        : (cursor - 1 + items.length) % items.length;
      items[cursor].setAttribute('aria-selected', 'true');
      items[cursor].scrollIntoView({ block: 'nearest' });
    } else if (event.key === 'Enter') {
      event.preventDefault();
      commit(cursor >= 0 ? cursor : 0);
    } else if (event.key === 'Escape') {
      close();
      input.blur();
    }
  });

  input.addEventListener('blur', () => setTimeout(close, 120));
}

/* ------------------------------------------------------- graph traversal */

/** A hub is collapsed everywhere except when you navigate directly to it. */
function isCollapsed(id) {
  return index.collapsible.has(id) && id !== state.root;
}

/** The collapsed utilities a node calls, shown as a badge instead of nodes. */
function badgesFor(id) {
  return (index.out.get(id) || [])
    .filter((edge) => edgeAllowed(edge) && isCollapsed(edge.target))
    .map((edge) => edge.target);
}

function nodeVisible(id) {
  const node = index.nodes.get(id);
  if (!node) return false;
  if (state.disabledPackages.has(node.package)) return false;
  if (state.hidePrivate && node.private && id !== state.root) return false;
  if (state.hideClasses && node.kind === 'class') return false;
  return true;
}

function edgeAllowed(edge) {
  return state.confidences.has(edge.confidence);
}

/** Neighbours of `id`, bridging transitively through filtered-out nodes.
 *
 * Filtered-out nodes are bridged so a chain never silently truncates, but
 * collapsed hubs terminate instead: they are leaf utilities, and walking into
 * them re-creates exactly the long edges collapsing them removed.
 */
function visibleNeighbors(id, direction) {
  const map = direction === 'callees' ? index.out : index.in;
  const found = new Map();
  const seen = new Set([id]);

  const walk = (current, bridged) => {
    (map.get(current) || []).forEach((edge) => {
      if (!edgeAllowed(edge)) return;
      const next = direction === 'callees' ? edge.target : edge.source;
      if (seen.has(next)) return;
      if (isCollapsed(next)) return;
      if (nodeVisible(next)) {
        const existing = found.get(next);
        if (!existing || (existing.bridged && !bridged)) {
          found.set(next, { edge, bridged });
        }
      } else {
        seen.add(next);
        walk(next, true);
      }
    });
  };

  walk(id, false);
  return found;
}

/** Keep the structurally interesting nodes when a rank is capped. */
function byInterest(left, right) {
  const weight = (id) => (index.out.get(id) || []).length;
  return weight(right[0]) - weight(left[0]) || left[0].localeCompare(right[0]);
}

/** Breadth-first subgraph around the current root. */
function subgraph() {
  const nodes = new Set();
  const edges = [];
  const edgeKeys = new Set();
  const overflow = new Map();
  const root = state.root;
  if (!root || !index.nodes.has(root)) return { nodes, edges, overflow };

  nodes.add(root);
  const directions = state.direction === 'both'
    ? ['callees', 'callers']
    : [state.direction];

  const addEdge = (current, neighbor, direction, meta) => {
    const source = direction === 'callees' ? current : neighbor;
    const target = direction === 'callees' ? neighbor : current;
    const key = `${source}->${target}`;
    if (edgeKeys.has(key)) return;
    edgeKeys.add(key);
    edges.push({
      key,
      source,
      target,
      confidence: meta.edge.confidence,
      lines: meta.edge.lines,
      bridged: meta.bridged,
    });
  };

  directions.forEach((direction) => {
    let frontier = [root];
    for (let level = 0; level < state.depth; level += 1) {
      // Collect the whole rank before deciding what fits. Capping per parent
      // would let several wide parents still stack into one unreadable rank.
      const candidates = [];
      frontier.forEach((current) => {
        visibleNeighbors(current, direction).forEach((meta, neighbor) => {
          candidates.push({ current, neighbor, meta });
        });
      });

      // Nodes already on screen cost no width, so their edges always land.
      const fresh = new Map();
      candidates.forEach((candidate) => {
        if (nodes.has(candidate.neighbor)) {
          addEdge(candidate.current, candidate.neighbor, direction, candidate.meta);
          return;
        }
        const entry = fresh.get(candidate.neighbor);
        if (entry) entry.parents.push(candidate);
        else fresh.set(candidate.neighbor, { parents: [candidate] });
      });

      const rankKey = `rank:${direction}:${level}`;
      let shown = [...fresh.entries()];
      let hidden = [];
      if (shown.length > MAX_RANK_WIDTH && !state.expanded.has(rankKey)) {
        const ranked = shown.slice().sort(byInterest);
        shown = ranked.slice(0, MAX_RANK_WIDTH);
        hidden = ranked.slice(MAX_RANK_WIDTH);
      }

      const nextFrontier = [];
      shown.forEach(([neighbor, entry]) => {
        nodes.add(neighbor);
        nextFrontier.push(neighbor);
        entry.parents.forEach((candidate) => {
          addEdge(candidate.current, neighbor, direction, candidate.meta);
        });
      });

      if (hidden.length) {
        const overflowId = `::more::${direction}::${level}`;
        overflow.set(overflowId, {
          key: rankKey,
          direction,
          hidden: hidden.map(([id]) => id),
        });
        nodes.add(overflowId);
        const parents = new Set();
        hidden.forEach(([, entry]) => {
          entry.parents.forEach((candidate) => parents.add(candidate.current));
        });
        parents.forEach((parent) => {
          const source = direction === 'callees' ? parent : overflowId;
          const target = direction === 'callees' ? overflowId : parent;
          const key = `${source}->${target}`;
          if (edgeKeys.has(key)) return;
          edgeKeys.add(key);
          edges.push({ key, source, target, overflow: true });
        });
      }

      frontier = nextFrontier;
      if (!frontier.length) break;
    }
  });

  return { nodes, edges, overflow };
}

function moduleSubgraph() {
  let visible = new Set();
  index.modules.forEach((module) => {
    if (!state.disabledPackages.has(module.package)) visible.add(module.id);
  });

  if (state.root && index.modules.has(state.root)) {
    const keep = new Set([state.root]);
    const directions = state.direction === 'both'
      ? ['moduleOut', 'moduleIn']
      : [state.direction === 'callees' ? 'moduleOut' : 'moduleIn'];
    directions.forEach((mapName) => {
      let frontier = [state.root];
      for (let level = 0; level < state.depth; level += 1) {
        const next = [];
        frontier.forEach((current) => {
          (index[mapName].get(current) || []).forEach((edge) => {
            const other = mapName === 'moduleOut' ? edge.target : edge.source;
            if (!visible.has(other) || keep.has(other)) return;
            keep.add(other);
            next.push(other);
          });
        });
        frontier = next;
        if (!frontier.length) break;
      }
    });
    visible = keep;
  }

  const edges = [];
  index.moduleOut.forEach((bucket) => {
    bucket.forEach((edge) => {
      if (visible.has(edge.source) && visible.has(edge.target)) {
        edges.push({
          key: `${edge.source}->${edge.target}`,
          source: edge.source,
          target: edge.target,
          weight: edge.weight,
        });
      }
    });
  });

  return { nodes: visible, edges };
}

/* -------------------------------------------------------------- rendering */

function initCytoscape() {
  cy = cytoscape({
    container: el('cy'),
    minZoom: 0.15,
    maxZoom: 2.5,
    wheelSensitivity: 0.25,
    style: [
      {
        selector: 'node',
        style: {
          'background-color': 'data(color)',
          'background-opacity': 0.16,
          'border-width': 1.5,
          'border-color': 'data(color)',
          shape: 'round-rectangle',
          label: 'data(label)',
          color: '#e6edf3',
          'font-family': 'ui-monospace, Menlo, Consolas, monospace',
          'font-size': 11,
          'text-valign': 'center',
          'text-halign': 'center',
          'text-wrap': 'wrap',
          'text-max-width': 190,
          width: 'data(w)',
          height: 'data(h)',
          padding: 6,
        },
      },
      {
        selector: 'node[kind = "class"]',
        style: { shape: 'cut-rectangle', 'border-style': 'dashed' },
      },
      {
        selector: 'node[kind = "module"]',
        style: { shape: 'round-rectangle', 'background-opacity': 0.22 },
      },
      {
        selector: 'node[?isOverflow]',
        style: {
          shape: 'round-rectangle',
          'background-opacity': 0.06,
          'border-style': 'dotted',
          'border-width': 1,
          color: '#9aa4b2',
          'font-size': 10,
        },
      },
      {
        selector: 'node[?isRoot]',
        style: {
          'border-width': 3,
          'background-opacity': 0.34,
          'font-weight': 'bold',
        },
      },
      {
        selector: 'node[?isEntry]',
        style: { 'border-color': '#58a6ff', 'border-width': 2.5 },
      },
      {
        selector: 'node:selected',
        style: { 'border-color': '#ffffff', 'border-width': 3 },
      },
      {
        selector: 'edge',
        style: {
          width: 'data(w)',
          'line-color': 'data(color)',
          'target-arrow-color': 'data(color)',
          'target-arrow-shape': 'triangle',
          'arrow-scale': 0.85,
          'curve-style': 'bezier',
          opacity: 0.55,
        },
      },
      {
        selector: 'edge[?bridged]',
        style: { 'line-style': 'dashed', opacity: 0.35 },
      },
      {
        selector: 'edge[?overflow]',
        style: { 'line-style': 'dotted', opacity: 0.3, 'target-arrow-shape': 'none' },
      },
      {
        selector: '.faded',
        style: { opacity: 0.12, 'text-opacity': 0.25 },
      },
      {
        selector: '.hot',
        style: { opacity: 1, 'text-opacity': 1, 'z-index': 20 },
      },
    ],
  });

  cy.on('tap', 'node', (event) => {
    const id = event.target.id();
    const spill = currentOverflow.get(id);
    if (spill) {
      state.expanded.add(spill.key);
      render();
      return;
    }
    state.selected = id;
    highlightNeighborhood(id);
    if (state.view === 'modules') showModuleDetail(id);
    else showNodeDetail(id);
  });

  cy.on('dbltap', 'node', (event) => {
    const id = event.target.id();
    if (currentOverflow.has(id)) return;
    if (state.view === 'modules') {
      const module = index.modules.get(id);
      const target = (module && module.symbols && module.symbols[0]) || null;
      if (target) {
        state.view = 'functions';
        syncViewButtons();
        setRoot(target, { reset: true });
      }
    } else {
      setRoot(id);
    }
  });

  cy.on('tap', (event) => {
    if (event.target === cy) {
      cy.elements().removeClass('faded hot');
    }
  });
}

function highlightNeighborhood(id) {
  const node = cy.getElementById(id);
  if (!node.length) return;
  const hood = node.closedNeighborhood();
  cy.elements().addClass('faded').removeClass('hot');
  hood.removeClass('faded').addClass('hot');
}

function measure(label) {
  const lines = label.split('\n');
  const longest = lines.reduce((max, line) => Math.max(max, line.length), 0);
  return {
    w: Math.min(210, Math.max(72, longest * 6.6 + 22)),
    h: 16 + lines.length * 13,
  };
}

function render() {
  if (!cy) return;

  const elements = [];

  if (state.view === 'modules') {
    const { nodes, edges } = moduleSubgraph();
    currentOverflow = new Map();
    nodes.forEach((id) => {
      const module = index.modules.get(id);
      const label = shortModule(id);
      const { w, h } = measure(label);
      elements.push({
        data: {
          id,
          label,
          kind: 'module',
          color: packageColor(module.package),
          w: Math.max(w, 90),
          h,
          isRoot: id === state.root || undefined,
          isEntry: module.entryWorkflows.length ? true : undefined,
        },
      });
    });
    edges.forEach((edge) => {
      elements.push({
        data: {
          id: edge.key,
          source: edge.source,
          target: edge.target,
          color: '#4d5866',
          w: Math.min(6, 1 + Math.log2(edge.weight + 1)),
        },
      });
    });
  } else {
    const { nodes, edges, overflow } = subgraph();
    currentOverflow = overflow;
    nodes.forEach((id) => {
      const spill = overflow.get(id);
      if (spill) {
        const label = `+${spill.hidden.length} more`;
        const { w, h } = measure(label);
        elements.push({
          data: {
            id,
            label,
            kind: 'overflow',
            color: '#6b7684',
            w,
            h,
            isOverflow: true,
          },
        });
        return;
      }

      const node = index.nodes.get(id);
      if (!node) return;
      const badges = badgesFor(id);
      const label = badges.length
        ? `${shortName(id)}\n${shortModule(node.module)}\n⚙ ${badges.length}`
        : `${shortName(id)}\n${shortModule(node.module)}`;
      const { w, h } = measure(label);
      elements.push({
        data: {
          id,
          label,
          kind: node.kind,
          color: packageColor(node.package),
          w,
          h,
          isRoot: id === state.root || undefined,
          isEntry: node.entryWorkflows ? true : undefined,
        },
      });
    });
    edges.forEach((edge) => {
      elements.push({
        data: {
          id: edge.key,
          source: edge.source,
          target: edge.target,
          color: edge.overflow ? '#4d5866' : (CONFIDENCE_COLOR[edge.confidence] || '#4d5866'),
          w: 1.4,
          bridged: edge.bridged || undefined,
          overflow: edge.overflow || undefined,
        },
      });
    });
  }

  cy.elements().remove();
  cy.add(elements);
  runLayout();

  renderTrail();

  if (state.selected && cy.getElementById(state.selected).length) {
    cy.getElementById(state.selected).select();
  }
  updateHash();
}

/** Dagre gives the clearest call-flow reading; fall back if it failed to load. */
function runLayout() {
  const dagreOptions = {
    name: 'dagre',
    rankDir: 'LR',
    nodeSep: 18,
    rankSep: 90,
    edgeSep: 8,
    animate: false,
    fit: true,
    padding: 40,
  };
  try {
    cy.layout(dagreOptions).run();
  } catch (error) {
    cy.layout({
      name: 'breadthfirst',
      directed: true,
      animate: false,
      fit: true,
      padding: 40,
    }).run();
  }
}

function renderTrail() {
  const trail = el('trail');
  if (state.view === 'modules' && !state.root) {
    trail.innerHTML = '<button disabled>all modules</button>';
    return;
  }
  const crumbs = state.trail.length ? state.trail : (state.root ? [state.root] : []);
  trail.innerHTML = crumbs.map((id, i) => {
    const label = state.view === 'modules' ? shortModule(id) : shortName(id);
    const sep = i < crumbs.length - 1 ? '<span class="sep">›</span>' : '';
    return `<button data-i="${i}" title="${escapeHtml(id)}">${escapeHtml(label)}</button>${sep}`;
  }).join('');

  trail.querySelectorAll('button[data-i]').forEach((button) => {
    button.addEventListener('click', () => {
      const i = Number(button.dataset.i);
      state.trail = state.trail.slice(0, i + 1);
      state.root = state.trail[i];
      render();
      if (state.view === 'modules') showModuleDetail(state.root);
      else showNodeDetail(state.root);
    });
  });
}

function setRoot(id, options = {}) {
  state.root = id;
  if (options.reset) {
    state.trail = id ? [id] : [];
    state.expanded.clear();
  } else if (id && state.trail[state.trail.length - 1] !== id) {
    state.trail.push(id);
  }

  state.selected = id;
  el('entrypoints').querySelectorAll('li').forEach((item) => {
    item.setAttribute('aria-current', String(item.dataset.node === id));
  });

  render();
  if (!id) return;
  if (state.view === 'modules') showModuleDetail(id);
  else showNodeDetail(id);
}

/* ---------------------------------------------------------- detail panel */

function highlightPython(source) {
  if (window.hljs && window.hljs.getLanguage && window.hljs.getLanguage('python')) {
    try {
      return window.hljs.highlight(source, { language: 'python' }).value;
    } catch (error) {
      /* fall through to plain text */
    }
  }
  return escapeHtml(source);
}

function callList(edges, direction) {
  if (!edges || !edges.length) {
    return '<p class="d-empty">none</p>';
  }
  const rows = edges.slice().sort((a, b) => {
    const left = direction === 'out' ? a.target : a.source;
    const right = direction === 'out' ? b.target : b.source;
    return left.localeCompare(right);
  });

  return `<ul class="d-list">${rows.map((edge) => {
    const other = direction === 'out' ? edge.target : edge.source;
    const node = index.nodes.get(other);
    const color = CONFIDENCE_COLOR[edge.confidence] || '#4d5866';
    const where = node ? shortModule(node.module) : '';
    const lines = edge.lines && edge.lines.length
      ? `L${edge.lines[0]}${edge.lines.length > 1 ? `+${edge.lines.length - 1}` : ''}`
      : '';
    const collapsed = direction === 'out' && index.collapsible.has(other)
      ? '<span class="gear" title="shown as a badge, not a node">⚙</span> '
      : '';
    return `<li data-goto="${escapeHtml(other)}" title="${escapeHtml(other)} (${edge.confidence})">
      <span class="cbar" style="background:${color}"></span>
      <span class="tgt">${collapsed}${escapeHtml(shortName(other))}
        <span class="mod">· ${escapeHtml(where)}</span></span>
      <span class="ln">${lines}</span>
    </li>`;
  }).join('')}</ul>`;
}

function wireDetailLinks() {
  el('detail-body').querySelectorAll('[data-goto]').forEach((item) => {
    item.addEventListener('click', () => {
      state.view = 'functions';
      syncViewButtons();
      setRoot(item.dataset.goto);
    });
  });
}

function showNodeDetail(id) {
  const node = index.nodes.get(id);
  const body = el('detail-body');
  const empty = el('detail-empty');
  if (!node) {
    body.hidden = true;
    empty.hidden = false;
    return;
  }
  empty.hidden = true;
  body.hidden = false;

  const outgoing = index.out.get(id) || [];
  const incoming = index.in.get(id) || [];
  const lineRange = node.endLine > node.line ? `L${node.line}-L${node.endLine}` : `L${node.line}`;
  const workflows = (node.entryWorkflows || [])
    .map((wf) => `<span class="wf-chip">${escapeHtml(wf)}</span>`).join('');
  const externals = (node.externals || [])
    .map((name) => `<span class="chip">${escapeHtml(name)}</span>`).join('');

  body.innerHTML = `
    <span class="d-kind">${escapeHtml(node.kind)}${node.async ? ' · async' : ''}</span>
    <h2 class="d-name">${escapeHtml(node.qualname)}</h2>
    <div class="d-loc">
      ${escapeHtml(shortModule(node.module))} ·
      <a href="${REPO_BLOB}/${escapeHtml(node.file)}#${lineRange}" target="_blank"
         rel="noopener">${escapeHtml(node.file)}:${node.line}</a>
      · ${node.loc} lines
    </div>
    ${workflows ? `<div class="d-section"><h3>Started by</h3>${workflows}</div>` : ''}
    <pre class="d-sig">${escapeHtml(node.signature)}</pre>
    <p class="d-doc${node.doc ? '' : ' none'}">${escapeHtml(node.doc || 'No docstring.')}</p>

    <div class="d-section">
      <h3>Calls (${outgoing.length})</h3>
      ${callList(outgoing, 'out')}
    </div>

    <div class="d-section">
      <h3>Called by (${incoming.length})</h3>
      ${callList(incoming, 'in')}
    </div>

    ${externals ? `<div class="d-section"><h3>External effects</h3>
      <div class="chips">${externals}</div></div>` : ''}

    <div class="d-section">
      <h3>Source</h3>
      <div class="d-source"><pre><code class="language-python">${
        highlightPython(node.source)
      }</code></pre></div>
    </div>`;

  wireDetailLinks();
}

function showModuleDetail(id) {
  const module = index.modules.get(id);
  const body = el('detail-body');
  const empty = el('detail-empty');
  if (!module) {
    body.hidden = true;
    empty.hidden = false;
    return;
  }
  empty.hidden = true;
  body.hidden = false;

  const outgoing = index.moduleOut.get(id) || [];
  const incoming = index.moduleIn.get(id) || [];
  const workflows = module.entryWorkflows
    .map((wf) => `<span class="wf-chip">${escapeHtml(wf)}</span>`).join('');

  const moduleList = (edges, direction) => {
    if (!edges.length) return '<p class="d-empty">none</p>';
    return `<ul class="d-list">${edges.slice()
      .sort((a, b) => b.weight - a.weight)
      .map((edge) => {
        const other = direction === 'out' ? edge.target : edge.source;
        return `<li data-module="${escapeHtml(other)}">
          <span class="tgt">${escapeHtml(shortModule(other))}</span>
          <span class="ln">${edge.weight} call${edge.weight === 1 ? '' : 's'}</span>
        </li>`;
      }).join('')}</ul>`;
  };

  const symbols = module.symbols
    .map((symbolId) => {
      const node = index.nodes.get(symbolId);
      if (!node) return '';
      return `<li data-goto="${escapeHtml(symbolId)}" title="${escapeHtml(node.signature)}">
        <span class="tgt">${escapeHtml(node.name)}</span>
        <span class="ln">${node.kind === 'class' ? 'class' : `${node.callers}&larr;`}</span>
      </li>`;
    }).join('');

  body.innerHTML = `
    <span class="d-kind">module · ${escapeHtml(module.package)}</span>
    <h2 class="d-name">${escapeHtml(shortModule(id))}</h2>
    <div class="d-loc">
      <a href="${REPO_BLOB}/${escapeHtml(module.file)}" target="_blank"
         rel="noopener">${escapeHtml(module.file)}</a>
      · ${module.loc} lines · ${module.symbolCount} symbols
    </div>
    ${workflows ? `<div class="d-section"><h3>Entry point for</h3>${workflows}</div>` : ''}
    <p class="d-doc${module.doc ? '' : ' none'}">${escapeHtml(module.doc || 'No module docstring.')}</p>

    <div class="d-section">
      <h3>Depends on (${outgoing.length})</h3>
      ${moduleList(outgoing, 'out')}
    </div>

    <div class="d-section">
      <h3>Used by (${incoming.length})</h3>
      ${moduleList(incoming, 'in')}
    </div>

    <div class="d-section">
      <h3>Defines (${module.symbols.length})</h3>
      <ul class="d-list">${symbols}</ul>
    </div>`;

  body.querySelectorAll('[data-module]').forEach((item) => {
    item.addEventListener('click', () => setRoot(item.dataset.module));
  });
  wireDetailLinks();
}

/* ------------------------------------------------------------------ hash */

function updateHash() {
  const params = new URLSearchParams();
  params.set('view', state.view);
  if (state.root) params.set('root', state.root);
  params.set('depth', String(state.depth));
  params.set('dir', state.direction);
  const next = `#${params.toString()}`;
  if (window.location.hash !== next) {
    window.history.replaceState(null, '', next);
  }
}

function restoreFromHash() {
  const raw = window.location.hash.replace(/^#/, '');
  if (!raw) return false;
  const params = new URLSearchParams(raw);
  const view = params.get('view');
  const root = params.get('root');
  const depth = Number(params.get('depth'));
  const direction = params.get('dir');

  if (view === 'modules' || view === 'functions') state.view = view;
  if (depth >= 1 && depth <= 6) {
    state.depth = depth;
    el('depth').value = String(depth);
    el('depth-out').textContent = depth;
  }
  if (direction && ['callees', 'callers', 'both'].includes(direction)) {
    state.direction = direction;
    el('direction').value = direction;
  }
  syncViewButtons();

  const known = state.view === 'modules'
    ? index.modules.has(root)
    : index.nodes.has(root);
  if (root && known) {
    setRoot(root, { reset: true });
    return true;
  }
  if (state.view === 'modules') {
    setRoot(null, { reset: true });
    return true;
  }
  return false;
}

load();
