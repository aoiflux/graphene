package viz

import (
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"os"
	"sort"
	"strconv"

	"github.com/aoiflux/graphene/store"
)

type nodeVM struct {
	ID    string  `json:"id"`
	Label string  `json:"label"`
	Type  string  `json:"type"`
	Color string  `json:"color"`
	X     float64 `json:"x"`
	Y     float64 `json:"y"`
}

type edgeVM struct {
	ID     string  `json:"id"`
	Source string  `json:"source"`
	Target string  `json:"target"`
	Type   string  `json:"type"`
	Label  string  `json:"label"`
	Weight float32 `json:"weight"`
}

type pageData struct {
	Title      string
	Subtitle   string
	NodeCount  int
	EdgeCount  int
	NodesJSON  template.JS
	EdgesJSON  template.JS
	EdgeTypes  []string
	ColorItems []legendItem
}

// ExportOptions customizes metadata shown in the generated visualization.
type ExportOptions struct {
	Title    string
	Subtitle string
}

type legendItem struct {
	Name  string
	Color string
}

// ExportInteractiveHTML writes an interactive graph visualization to outPath.
// The output is a self-contained HTML file with embedded CSS/JS.
func ExportInteractiveHTML(nodes []*store.Node, edges []*store.Edge, outPath string) error {
	return ExportInteractiveHTMLWithOptions(nodes, edges, outPath, ExportOptions{})
}

// ExportInteractiveHTMLWithOptions writes an interactive graph visualization to outPath.
// The output is a self-contained HTML file with embedded CSS/JS.
func ExportInteractiveHTMLWithOptions(nodes []*store.Node, edges []*store.Edge, outPath string, opts ExportOptions) error {
	if len(nodes) == 0 {
		return fmt.Errorf("viz export requires at least one node")
	}

	nodesVM := make([]nodeVM, 0, len(nodes))
	nodeSet := make(map[store.NodeID]struct{}, len(nodes))
	for _, n := range nodes {
		nodeSet[n.ID] = struct{}{}
	}

	const (
		width  = 1500.0
		height = 980.0
		cx     = width / 2
		cy     = height / 2
		radius = 360.0
	)

	for i, n := range nodes {
		typeName := nodeTypeName(n)
		angle := 2 * math.Pi * float64(i) / float64(len(nodes))
		nodesVM = append(nodesVM, nodeVM{
			ID:    strconv.FormatUint(uint64(n.ID), 10),
			Label: typeName,
			Type:  typeName,
			Color: nodeColor(n),
			X:     cx + radius*math.Cos(angle),
			Y:     cy + radius*math.Sin(angle),
		})
	}

	edgesVM := make([]edgeVM, 0, len(edges))
	edgeTypesSet := make(map[string]struct{})
	for _, e := range edges {
		if _, ok := nodeSet[e.Src]; !ok {
			continue
		}
		if _, ok := nodeSet[e.Dst]; !ok {
			continue
		}

		t := edgeTypeName(e)
		edgeTypesSet[t] = struct{}{}
		edgesVM = append(edgesVM, edgeVM{
			ID:     strconv.FormatUint(uint64(e.ID), 10),
			Source: strconv.FormatUint(uint64(e.Src), 10),
			Target: strconv.FormatUint(uint64(e.Dst), 10),
			Type:   t,
			Label:  t,
			Weight: e.Weight,
		})
	}

	edgeTypes := make([]string, 0, len(edgeTypesSet))
	for t := range edgeTypesSet {
		edgeTypes = append(edgeTypes, t)
	}
	sort.Strings(edgeTypes)

	nodesJSON, err := json.Marshal(nodesVM)
	if err != nil {
		return err
	}
	edgesJSON, err := json.Marshal(edgesVM)
	if err != nil {
		return err
	}

	title := opts.Title
	if title == "" {
		title = "GrapheneDB Interactive Graph View"
	}
	subtitle := opts.Subtitle
	if subtitle == "" {
		subtitle = "Interactive connected-data view generated from GrapheneDB. Click a node for details, filter edge types, and zoom/pan the graph."
	}

	data := pageData{
		Title:     title,
		Subtitle:  subtitle,
		NodeCount: len(nodesVM),
		EdgeCount: len(edgesVM),
		NodesJSON: template.JS(nodesJSON),
		EdgesJSON: template.JS(edgesJSON),
		EdgeTypes: edgeTypes,
		ColorItems: []legendItem{
			{Name: "MicroArtefact", Color: "#0b5fff"},
			{Name: "EvidenceFile", Color: "#089981"},
			{Name: "Case", Color: "#d97706"},
			{Name: "Tag", Color: "#7c3aed"},
			{Name: "Other", Color: "#334155"},
		},
	}

	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	t, err := template.New("graphviz").Parse(pageTemplate)
	if err != nil {
		return err
	}
	if err := t.Execute(f, data); err != nil {
		return err
	}
	return nil
}

func nodeTypeName(n *store.Node) string {
	if len(n.Labels) == 0 {
		return "Unknown"
	}
	return n.Labels[0].String()
}

func edgeTypeName(e *store.Edge) string {
	if len(e.Labels) == 0 {
		return "Unknown"
	}
	return e.Labels[0].String()
}

func nodeColor(n *store.Node) string {
	if n.HasLabel(store.NodeTypeMicroArtefact) {
		return "#0b5fff"
	}
	if n.HasLabel(store.NodeTypeEvidenceFile) {
		return "#089981"
	}
	if n.HasLabel(store.NodeTypeCase) {
		return "#d97706"
	}
	if n.HasLabel(store.NodeTypeTag) {
		return "#7c3aed"
	}
	return "#334155"
}

const pageTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{.Title}}</title>
  <style>
    :root {
      --bg: #eaf1f8;
      --panel: #ffffff;
      --ink: #111827;
      --muted: #4b5563;
      --line: #dbe4ee;
      --accent: #0b5fff;
      --good: #089981;
      --warm: #f59e0b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 5% 10%, rgba(11,95,255,0.12), transparent 30%),
        radial-gradient(circle at 92% 88%, rgba(8,153,129,0.14), transparent 28%),
        radial-gradient(circle at 80% 20%, rgba(245,158,11,0.12), transparent 22%),
        var(--bg);
    }
    .container {
      max-width: 1580px;
      margin: 18px auto;
      padding: 0 14px;
      display: grid;
      grid-template-columns: 320px 1fr;
      gap: 14px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 14px;
      box-shadow: 0 12px 34px rgba(17,24,39,0.08);
    }
    .controls {
      padding: 14px;
      position: sticky;
      top: 14px;
      height: fit-content;
    }
    h1 {
      margin: 0 0 8px 0;
      font-size: 20px;
      letter-spacing: 0.2px;
    }
    .sub {
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 12px;
      line-height: 1.35;
    }
    .statRow {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
      margin-bottom: 12px;
    }
    .stat {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px 10px;
      background: #fbfdff;
    }
    .stat b { font-size: 16px; }
    .label {
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 5px;
      display: block;
    }
    input[type="text"] {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 9px 10px;
      font-size: 13px;
      outline: none;
      margin-bottom: 10px;
    }
    input[type="range"] {
      width: 100%;
      margin-bottom: 12px;
      accent-color: var(--accent);
    }
    input[type="text"]:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px rgba(11,95,255,0.12);
    }
    .edgeFilters {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      max-height: 180px;
      overflow: auto;
      margin-bottom: 12px;
      background: #fbfdff;
    }
    .chk {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      margin: 5px 0;
      user-select: none;
    }
    .row {
      display: flex;
      gap: 8px;
      margin-bottom: 12px;
    }
    button {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
      background: #fff;
      cursor: pointer;
    }
    button:hover {
      border-color: var(--accent);
      color: var(--accent);
    }
    .legend {
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      background: #fbfdff;
      margin-bottom: 12px;
    }
    .legendItem {
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 5px 0;
      font-size: 13px;
    }
    .dot {
      width: 10px;
      height: 10px;
      border-radius: 99px;
      display: inline-block;
    }
    .details {
      border: 1px dashed var(--line);
      border-radius: 10px;
      padding: 10px;
      min-height: 86px;
      font-size: 12px;
      color: var(--muted);
      background: #fbfdff;
      line-height: 1.4;
      white-space: pre-line;
    }
    .stage {
      overflow: hidden;
      position: relative;
      min-height: 900px;
    }
    svg {
      width: 100%;
      height: 900px;
      display: block;
      border-radius: 14px;
      background: linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
    }
    .edge {
      stroke: #9aa8bb;
      stroke-width: 1.2;
      opacity: 0.8;
    }
    .edge.hide { opacity: 0.06; }
    .node {
      stroke: #0f172a;
      stroke-width: 1;
      cursor: pointer;
    }
    .node.dim {
      opacity: 0.18;
    }
    .node.active {
      stroke: #000;
      stroke-width: 2;
      filter: drop-shadow(0 0 6px rgba(11,95,255,0.35));
    }
    .nodeLabel {
      font-size: 11px;
      fill: #0f172a;
      pointer-events: none;
      user-select: none;
    }
    .hint {
      position: absolute;
      right: 12px;
      bottom: 12px;
      font-size: 12px;
      color: #475569;
      background: rgba(255,255,255,0.92);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 6px 8px;
    }
    @media (max-width: 1100px) {
      .container { grid-template-columns: 1fr; }
      .controls { position: static; }
      .stage { min-height: 640px; }
      svg { height: 640px; }
    }
  </style>
</head>
<body>
  <div class="container">
    <aside class="panel controls">
      <h1>{{.Title}}</h1>
      <div class="sub">{{.Subtitle}}</div>

      <div class="statRow">
        <div class="stat"><span class="label">Nodes</span><b>{{.NodeCount}}</b></div>
        <div class="stat"><span class="label">Edges</span><b>{{.EdgeCount}}</b></div>
      </div>

      <label class="label" for="search">Search node by ID or type</label>
      <input id="search" type="text" placeholder="e.g. 5021 or MicroArtefact" />

      <label class="label">Edge type filters</label>
      <div class="edgeFilters" id="edgeFilters">
        {{range .EdgeTypes}}
        <label class="chk"><input type="checkbox" value="{{.}}" checked /> {{.}}</label>
        {{end}}
      </div>

      <div class="row">
        <button id="resetView">Reset View</button>
        <button id="toggleLabels">Toggle Labels</button>
      </div>

      <div class="row">
        <button id="focusSelected">Focus Selection</button>
        <button id="downloadSVG">Download SVG</button>
      </div>

      <label class="label" for="nodeSize">Node size</label>
      <input id="nodeSize" type="range" min="5" max="16" step="1" value="9" />

      <div class="legend">
        <span class="label">Node legend</span>
        {{range .ColorItems}}
        <div class="legendItem"><span class="dot" style="background: {{.Color}}"></span>{{.Name}}</div>
        {{end}}
      </div>

      <div class="details" id="details">Hover or click on a node/edge to inspect details.</div>
    </aside>

    <section class="panel stage">
      <svg id="svg" viewBox="0 0 1500 980" role="img" aria-label="Interactive graph visualization">
        <g id="viewport">
          <g id="edges"></g>
          <g id="nodes"></g>
          <g id="labels"></g>
        </g>
      </svg>
      <div class="hint">Mouse wheel: zoom | Drag background: pan | Drag node: reposition</div>
    </section>
  </div>

  <script>
    const nodes = {{.NodesJSON}};
    const edges = {{.EdgesJSON}};

    const svg = document.getElementById('svg');
    const viewport = document.getElementById('viewport');
    const edgesLayer = document.getElementById('edges');
    const nodesLayer = document.getElementById('nodes');
    const labelsLayer = document.getElementById('labels');
    const details = document.getElementById('details');
    const search = document.getElementById('search');
    const edgeFilters = document.getElementById('edgeFilters');
    const resetViewBtn = document.getElementById('resetView');
    const toggleLabelsBtn = document.getElementById('toggleLabels');
    const focusSelectedBtn = document.getElementById('focusSelected');
    const downloadSVGBtn = document.getElementById('downloadSVG');
    const nodeSize = document.getElementById('nodeSize');

    const nodeByID = new Map(nodes.map(n => [n.id, n]));
    const adjacency = new Map();
    nodes.forEach(n => adjacency.set(n.id, new Set()));
    edges.forEach(e => {
      if (!adjacency.has(e.source)) adjacency.set(e.source, new Set());
      if (!adjacency.has(e.target)) adjacency.set(e.target, new Set());
      adjacency.get(e.source).add(e.target);
      adjacency.get(e.target).add(e.source);
    });

    const state = {
      zoom: 1,
      panX: 0,
      panY: 0,
      draggingStage: false,
      draggingNode: null,
      lastX: 0,
      lastY: 0,
      showLabels: true,
      activeTypes: new Set([...new Set(edges.map(e => e.type))]),
      selectedNode: null,
      nodeRadius: 9,
    };

    const nodeEls = new Map();
    const labelEls = new Map();
    const edgeEls = [];

    function createSVG(tag) {
      return document.createElementNS('http://www.w3.org/2000/svg', tag);
    }

    function render() {
      edgesLayer.innerHTML = '';
      nodesLayer.innerHTML = '';
      labelsLayer.innerHTML = '';
      nodeEls.clear();
      labelEls.clear();
      edgeEls.length = 0;

      edges.forEach(e => {
        const s = nodeByID.get(e.source);
        const t = nodeByID.get(e.target);
        if (!s || !t) return;

        const line = createSVG('line');
        line.setAttribute('class', 'edge');
        line.setAttribute('x1', s.x.toFixed(2));
        line.setAttribute('y1', s.y.toFixed(2));
        line.setAttribute('x2', t.x.toFixed(2));
        line.setAttribute('y2', t.y.toFixed(2));
        line.dataset.type = e.type;

        line.addEventListener('mouseenter', () => {
          details.textContent = 'Edge\nID: ' + e.id + '\nType: ' + e.type + '\nSource: ' + e.source + '\nTarget: ' + e.target + '\nWeight: ' + Number(e.weight || 0).toFixed(3);
        });

        edgesLayer.appendChild(line);
        edgeEls.push({ model: e, el: line });
      });

      nodes.forEach(n => {
        const c = createSVG('circle');
        c.setAttribute('class', 'node');
        c.setAttribute('cx', n.x.toFixed(2));
        c.setAttribute('cy', n.y.toFixed(2));
        c.setAttribute('r', String(state.nodeRadius));
        c.setAttribute('fill', n.color);
        c.dataset.id = n.id;

        c.addEventListener('mouseenter', () => {
          details.textContent = 'Node\nID: ' + n.id + '\nType: ' + n.type + '\nDegree: ' + (adjacency.get(n.id) || new Set()).size;
        });

        c.addEventListener('click', (ev) => {
          ev.stopPropagation();
          state.selectedNode = n.id;
          highlightNode(n.id);
          details.textContent = 'Node (selected)\nID: ' + n.id + '\nType: ' + n.type + '\nDegree: ' + (adjacency.get(n.id) || new Set()).size + '\nNeighbours: ' + Array.from(adjacency.get(n.id) || []).slice(0, 10).join(', ');
        });

        c.addEventListener('mousedown', (ev) => {
          ev.stopPropagation();
          state.draggingNode = n.id;
          state.lastX = ev.clientX;
          state.lastY = ev.clientY;
        });

        nodesLayer.appendChild(c);
        nodeEls.set(n.id, c);

        const label = createSVG('text');
        label.setAttribute('class', 'nodeLabel');
        label.setAttribute('x', (n.x + 11).toFixed(2));
        label.setAttribute('y', (n.y - 9).toFixed(2));
        label.textContent = n.type + ' #' + n.id;
        labelsLayer.appendChild(label);
        labelEls.set(n.id, label);
      });

      applyEdgeFilters();
      applySearch();
      applyViewport();
      applyLabelVisibility();
    }

    function applyViewport() {
      viewport.setAttribute('transform', 'translate(' + state.panX.toFixed(2) + ' ' + state.panY.toFixed(2) + ') scale(' + state.zoom.toFixed(4) + ')');
    }

    function applyLabelVisibility() {
      labelsLayer.style.display = state.showLabels ? 'block' : 'none';
    }

    function highlightNode(id) {
      nodeEls.forEach((el, nodeID) => {
        el.classList.toggle('active', nodeID === id);
      });
    }

    function applyEdgeFilters() {
      edgeEls.forEach(({ model, el }) => {
        const show = state.activeTypes.has(model.type);
        el.classList.toggle('hide', !show);
        const w = Number(model.weight || 0);
        el.style.strokeWidth = (1.1 + Math.min(w, 1) * 2.1).toFixed(2);
        el.style.opacity = show ? (0.25 + Math.min(w, 1) * 0.75).toFixed(2) : '0.06';
      });
    }

    function applySearch() {
      const q = search.value.trim().toLowerCase();
      if (!q) {
        nodeEls.forEach(el => el.classList.remove('dim'));
        return;
      }
      nodeEls.forEach((el, id) => {
        const n = nodeByID.get(id);
        const matched = n.id.toLowerCase().includes(q) || n.type.toLowerCase().includes(q);
        el.classList.toggle('dim', !matched);
      });
    }

    function screenToGraph(clientX, clientY) {
      const rect = svg.getBoundingClientRect();
      const sx = ((clientX - rect.left) / rect.width) * 1500;
      const sy = ((clientY - rect.top) / rect.height) * 980;
      return {
        x: (sx - state.panX) / state.zoom,
        y: (sy - state.panY) / state.zoom,
      };
    }

    function focusOnSelected() {
      if (!state.selectedNode) return;
      const n = nodeByID.get(state.selectedNode);
      if (!n) return;
      state.panX = 750 - n.x * state.zoom;
      state.panY = 490 - n.y * state.zoom;
      applyViewport();
    }

    svg.addEventListener('mousedown', (ev) => {
      if (state.draggingNode) return;
      state.draggingStage = true;
      state.lastX = ev.clientX;
      state.lastY = ev.clientY;
    });

    window.addEventListener('mousemove', (ev) => {
      if (state.draggingNode) {
        const id = state.draggingNode;
        const n = nodeByID.get(id);
        if (!n) return;
        const p = screenToGraph(ev.clientX, ev.clientY);
        n.x = p.x;
        n.y = p.y;
        render();
        highlightNode(id);
        return;
      }

      if (!state.draggingStage) return;
      const dx = ev.clientX - state.lastX;
      const dy = ev.clientY - state.lastY;
      state.panX += dx * (1500 / svg.getBoundingClientRect().width);
      state.panY += dy * (980 / svg.getBoundingClientRect().height);
      state.lastX = ev.clientX;
      state.lastY = ev.clientY;
      applyViewport();
    });

    window.addEventListener('mouseup', () => {
      state.draggingStage = false;
      state.draggingNode = null;
    });

    svg.addEventListener('wheel', (ev) => {
      ev.preventDefault();
      const factor = ev.deltaY < 0 ? 1.08 : 0.92;
      const next = Math.min(4.2, Math.max(0.35, state.zoom * factor));
      state.zoom = next;
      applyViewport();
    }, { passive: false });

    edgeFilters.addEventListener('change', () => {
      const checked = edgeFilters.querySelectorAll('input[type="checkbox"]:checked');
      state.activeTypes = new Set(Array.from(checked).map(c => c.value));
      applyEdgeFilters();
    });

    search.addEventListener('input', applySearch);

    resetViewBtn.addEventListener('click', () => {
      state.zoom = 1;
      state.panX = 0;
      state.panY = 0;
      state.selectedNode = null;
      highlightNode('');
      applyViewport();
      details.textContent = 'View reset. Hover or click a node/edge to inspect details.';
    });

    toggleLabelsBtn.addEventListener('click', () => {
      state.showLabels = !state.showLabels;
      applyLabelVisibility();
    });

    focusSelectedBtn.addEventListener('click', focusOnSelected);

    nodeSize.addEventListener('input', () => {
      state.nodeRadius = Number(nodeSize.value || '9');
      render();
      if (state.selectedNode) {
        highlightNode(state.selectedNode);
      }
    });

    downloadSVGBtn.addEventListener('click', () => {
      const copy = svg.cloneNode(true);
      copy.removeAttribute('style');
      copy.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
      const src = '<?xml version="1.0" standalone="no"?>\n' + copy.outerHTML;
      const blob = new Blob([src], { type: 'image/svg+xml;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'graphene-visualization.svg';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    });

    svg.addEventListener('click', () => {
      state.selectedNode = null;
      highlightNode('');
    });

    render();
  </script>
</body>
</html>`
