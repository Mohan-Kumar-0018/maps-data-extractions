"""HTTP server serving a static data-summary dashboard with Leaflet map."""

import json
import logging
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler

from scraper.db import ListingsDB

logger = logging.getLogger(__name__)

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Maps Data Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: #f5f5f5; color: #333;
  }
  .header {
    background: #1976d2; color: #fff; padding: 18px 28px;
    font-size: 22px; font-weight: 600;
  }
  .container { max-width: 1400px; margin: 0 auto; padding: 20px; }

  /* Stat cards */
  .cards { display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
  .card {
    flex: 1; min-width: 200px; background: #fff; border-radius: 8px;
    padding: 18px 22px; box-shadow: 0 1px 4px rgba(0,0,0,0.1);
  }
  .card-label { font-size: 13px; color: #777; text-transform: uppercase; letter-spacing: 0.5px; }
  .card-value { font-size: 32px; font-weight: 700; margin-top: 4px; }
  .card-sub { font-size: 12px; color: #999; margin-top: 2px; }

  /* Two-column rows */
  .row { display: flex; gap: 20px; margin-bottom: 20px; flex-wrap: wrap; }
  .col { flex: 1; min-width: 300px; }
  .panel {
    background: #fff; border-radius: 8px; padding: 18px 22px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.1); height: 100%;
  }
  .panel h3 { font-size: 15px; margin-bottom: 12px; color: #555; }

  /* Funnel bars */
  .funnel-bar { margin-bottom: 10px; }
  .funnel-label { font-size: 13px; margin-bottom: 3px; display: flex; justify-content: space-between; }
  .funnel-track { background: #eee; border-radius: 4px; height: 24px; overflow: hidden; }
  .funnel-fill { height: 100%; border-radius: 4px; transition: width 0.5s; min-width: 2px; }

  /* Tables */
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 8px 10px; border-bottom: 2px solid #eee; color: #777; font-weight: 600; }
  td { padding: 7px 10px; border-bottom: 1px solid #f0f0f0; }
  tr:hover { background: #fafafa; }

  /* Map */
  #map { width: 100%; height: 500px; border-radius: 8px; }
  .map-panel { background: #fff; border-radius: 8px; padding: 18px 22px; box-shadow: 0 1px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }
  .map-panel h3 { font-size: 15px; margin-bottom: 12px; color: #555; }

  /* Category filters */
  .cat-filters { display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; }
  .cat-btn {
    display: inline-flex; align-items: center; gap: 5px;
    padding: 6px 14px; border: 2px solid #ddd; border-radius: 20px;
    background: #fff; font-size: 13px; cursor: pointer; transition: all 0.2s;
  }
  .cat-btn:hover { border-color: #aaa; }
  .cat-btn.active { border-color: #333; background: #333; color: #fff; }
  .cat-btn .cat-dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }

  /* Legend */
  .map-legend { display: flex; gap: 16px; margin-top: 10px; font-size: 12px; }
  .legend-item { display: flex; align-items: center; gap: 5px; }
  .legend-dot { width: 14px; height: 14px; border-radius: 50%; }
</style>
</head>
<body>
<div class="header">Maps Data Dashboard</div>
<div class="container">

  <!-- ROW 1: Stat cards -->
  <div class="cards">
    <div class="card">
      <div class="card-label">Sample Points</div>
      <div class="card-value" id="c-points">--</div>
      <div class="card-sub" id="c-points-sub"></div>
    </div>
    <div class="card">
      <div class="card-label">Businesses</div>
      <div class="card-value" id="c-biz">--</div>
      <div class="card-sub" id="c-biz-sub"></div>
    </div>
    <div class="card">
      <div class="card-label">Enriched</div>
      <div class="card-value" id="c-enriched">--</div>
      <div class="card-sub" id="c-enriched-sub"></div>
    </div>
    <div class="card">
      <div class="card-label">Contacts Found</div>
      <div class="card-value" id="c-contacts">--</div>
      <div class="card-sub" id="c-contacts-sub"></div>
    </div>
  </div>

  <!-- ROW 2: Pipeline funnel + Category table -->
  <div class="row">
    <div class="col">
      <div class="panel">
        <h3>Pipeline Funnel</h3>
        <div id="funnel"></div>
      </div>
    </div>
    <div class="col">
      <div class="panel">
        <h3>Category Breakdown</h3>
        <div style="overflow-x:auto;">
          <table id="cat-table">
            <thead><tr><th>Category</th><th>Mappings</th><th>Businesses</th><th>Enriched</th><th>Contacts</th></tr></thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- ROW 3: Map -->
  <div class="map-panel">
    <h3>Sample Point Results</h3>
    <div id="cat-filters" class="cat-filters"></div>
    <div id="map"></div>
    <div class="map-legend">
      <div class="legend-item"><span class="legend-dot" style="background:#f44336"></span> 0 results</div>
      <div class="legend-item"><span class="legend-dot" style="background:#ff9800"></span> Low</div>
      <div class="legend-item"><span class="legend-dot" style="background:#ffeb3b"></span> Medium</div>
      <div class="legend-item"><span class="legend-dot" style="background:#4caf50"></span> High</div>
      <div class="legend-item"><span class="legend-dot" style="background:#9e9e9e"></span> Pending</div>
    </div>
  </div>

  <!-- ROW 4: Duplicate analysis -->
  <div class="row">
    <div class="col">
      <div class="panel">
        <h3>Duplicate Distribution</h3>
        <div style="overflow-x:auto;">
          <table id="dup-dist-table">
            <thead><tr><th>Duplicate Count</th><th>Businesses</th></tr></thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
    </div>
    <div class="col">
      <div class="panel">
        <h3>Top Duplicated Businesses</h3>
        <div style="overflow-x:auto; max-height: 400px; overflow-y: auto;">
          <table id="dup-hot-table">
            <thead><tr><th>Name</th><th>Category</th><th>Duplicates</th></tr></thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- ROW 5: Zero-result points -->
  <div class="row">
    <div class="col" style="flex:unset; width:100%;">
      <div class="panel">
        <h3>Zero-Result Points</h3>
        <div style="overflow-x:auto; max-height: 300px; overflow-y: auto;">
          <table id="zero-table">
            <thead><tr><th>Point ID</th><th>Lat</th><th>Lng</th><th>Mappings</th></tr></thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

</div>

<script>
const map = L.map('map');
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19, attribution: '&copy; OpenStreetMap'
}).addTo(map);

// ── globals ──
let allPoints = [];
let allCategoryNames = [];
let pointLayers = [];

const CATEGORY_PALETTE = [
  '#1976d2', '#e91e63', '#4caf50', '#ff9800', '#9c27b0',
  '#00bcd4', '#795548', '#607d8b', '#f44336', '#8bc34a',
  '#3f51b5', '#009688', '#ff5722', '#cddc39',
];
function catColor(i) { return CATEGORY_PALETTE[i % CATEGORY_PALETTE.length]; }

// ── density color (for aggregate / single-category view) ──
function densityColor(count) {
  if (count === 0) return '#f44336';
  if (count <= 3) return '#ff9800';
  if (count <= 8) return '#ffeb3b';
  return '#4caf50';
}

// ── build category filter buttons ──
function buildFilters(categories) {
  const el = document.getElementById('cat-filters');
  let html = '<button class="cat-btn active" data-cat="__all__">All Categories</button>';
  categories.forEach((cat, i) => {
    html += '<button class="cat-btn" data-cat="' + cat + '">' +
      '<span class="cat-dot" style="background:' + catColor(i) + '"></span>' + cat + '</button>';
  });
  el.innerHTML = html;
  el.querySelectorAll('.cat-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      el.querySelectorAll('.cat-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      renderMap(btn.dataset.cat);
    });
  });
}

// ── build popup HTML for a point ──
function buildPopup(p, selectedCat) {
  let html = '<b>Point ' + p.id + '</b><br>' +
    'Lat: ' + p.lat.toFixed(6) + ', Lng: ' + p.lng.toFixed(6) + '<br>';
  if (selectedCat === '__all__') {
    html += 'Total results: ' + p.total_raw_results + '<br>' +
      'Unique: ' + p.unique_businesses + '<br>' +
      'Duplicates: ' + p.duplicate_hits + '<br>';
  }
  const cats = p.categories || {};
  const names = Object.keys(cats);
  if (names.length > 0) {
    html += '<hr style="margin:4px 0">';
    names.forEach(cat => {
      const cd = cats[cat];
      const bold = (selectedCat !== '__all__' && cat === selectedCat) ? 'font-weight:700;' : '';
      html += '<span style="' + bold + '">' + cat + ': ' +
        cd.unique_businesses + ' biz (' + cd.total_results + ' raw)</span><br>';
    });
  }
  return html;
}

// ── render map markers based on selected category ──
function renderMap(selectedCat) {
  pointLayers.forEach(l => map.removeLayer(l));
  pointLayers = [];

  allPoints.forEach(p => {
    const cats = p.categories || {};

    if (selectedCat === '__all__') {
      // Aggregate view — color by total density
      let color;
      if (p.mappings_done === 0) color = '#9e9e9e';
      else color = densityColor(p.unique_businesses);

      const circle = L.circleMarker([p.lat, p.lng], {
        radius: 8, color: color, fillColor: color, fillOpacity: 0.8, weight: 1,
      }).addTo(map);
      circle.bindPopup(buildPopup(p, selectedCat));
      pointLayers.push(circle);
    } else {
      // Single category view
      const cd = cats[selectedCat];
      let color, radius, opacity;
      if (!cd) {
        color = '#bdbdbd'; radius = 5; opacity = 0.4;
      } else {
        color = densityColor(cd.unique_businesses);
        radius = Math.max(6, Math.min(14, 6 + cd.unique_businesses));
        opacity = 0.85;
      }
      const circle = L.circleMarker([p.lat, p.lng], {
        radius: radius, color: color, fillColor: color,
        fillOpacity: opacity, weight: 1,
      }).addTo(map);
      circle.bindPopup(buildPopup(p, selectedCat));
      // Count label for non-zero
      if (cd && cd.unique_businesses > 0) {
        const label = L.marker([p.lat, p.lng], {
          icon: L.divIcon({
            className: '',
            html: '<div style="color:#fff;font-size:10px;font-weight:700;text-align:center;margin-top:-5px;">' +
              cd.unique_businesses + '</div>',
            iconSize: [20, 20],
          })
        }).addTo(map);
        pointLayers.push(label);
      }
      pointLayers.push(circle);
    }
  });
}

// ── funnel chart ──
function buildFunnel(stats) {
  const steps = [
    { label: 'Extracted', value: stats.total_businesses, color: '#1976d2' },
    { label: 'Enriched', value: stats.enriched_done, color: '#4caf50' },
    { label: 'Has Website', value: stats.businesses_with_websites, color: '#ff9800' },
    { label: 'Contacts Found', value: stats.contacts_with_data, color: '#9c27b0' },
  ];
  const maxVal = Math.max(1, steps[0].value);
  const el = document.getElementById('funnel');
  el.innerHTML = steps.map(s => {
    const pct = (s.value / maxVal * 100).toFixed(1);
    return '<div class="funnel-bar">' +
      '<div class="funnel-label"><span>' + s.label + '</span><span>' + s.value.toLocaleString() + '</span></div>' +
      '<div class="funnel-track"><div class="funnel-fill" style="width:' + pct + '%;background:' + s.color + '"></div></div>' +
      '</div>';
  }).join('');
}

function fillTable(id, rows, cols) {
  const tbody = document.querySelector('#' + id + ' tbody');
  tbody.innerHTML = rows.map(r =>
    '<tr>' + cols.map(c => '<td>' + (r[c] != null ? r[c].toLocaleString() : '--') + '</td>').join('') + '</tr>'
  ).join('');
}

// ── main load ──
async function load() {
  try {
    const res = await fetch('/summary.json');
    const d = await res.json();
    const s = d.overall;

    // Stat cards
    document.getElementById('c-points').textContent = s.total_sample_points.toLocaleString();
    document.getElementById('c-points-sub').textContent =
      s.mappings_done + ' done / ' + s.mappings_pending + ' pending / ' + s.mappings_failed + ' failed';
    document.getElementById('c-biz').textContent = s.total_businesses.toLocaleString();
    document.getElementById('c-biz-sub').textContent = s.total_duplicate_hits + ' duplicate hits';
    document.getElementById('c-enriched').textContent = s.enriched_done.toLocaleString();
    document.getElementById('c-enriched-sub').textContent = s.enriched_failed + ' failed';
    document.getElementById('c-contacts').textContent = s.contacts_with_data.toLocaleString();
    document.getElementById('c-contacts-sub').textContent =
      s.contacts_done + ' done / ' + s.contacts_failed + ' failed';

    // Funnel
    buildFunnel(s);

    // Category table
    fillTable('cat-table', d.categories, [
      'category', 'mappings_done', 'unique_businesses', 'enriched_count', 'contacts_done'
    ]);

    // Store data for map rendering
    allPoints = d.points;
    allCategoryNames = d.categories.map(c => c.category);

    // Category filter buttons
    buildFilters(allCategoryNames);

    // Polygon overlay
    if (d.polygon && d.polygon.length > 0) {
      L.polygon(d.polygon, {
        color: '#1976d2', weight: 2, fillOpacity: 0.08
      }).addTo(map);
    }

    // Fit map bounds
    if (d.points.length > 0) {
      const bounds = d.points.map(p => [p.lat, p.lng]);
      map.fitBounds(bounds, { padding: [30, 30] });
    } else {
      map.setView([24.7, 46.7], 10);
    }

    // Initial render — all categories
    renderMap('__all__');

    // Duplicate distribution
    fillTable('dup-dist-table', d.duplicate_distribution, ['duplicate_count', 'business_count']);

    // Duplicate hotspots
    fillTable('dup-hot-table', d.duplicate_hotspots, ['name', 'category', 'duplicate_count']);

    // Zero-result points
    if (d.zero_result_points.length === 0) {
      document.querySelector('#zero-table tbody').innerHTML =
        '<tr><td colspan="4" style="text-align:center;color:#999;">No zero-result points</td></tr>';
    } else {
      fillTable('zero-table', d.zero_result_points, ['id', 'lat', 'lng', 'total_mappings']);
    }
  } catch (e) {
    console.error('Failed to load summary:', e);
  }
}

load();
</script>
</body>
</html>"""


class _Handler(BaseHTTPRequestHandler):
    """Serves dashboard HTML and summary JSON."""

    def __init__(self, data_json: str, *args, **kwargs):
        self._data_json = data_json
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self._respond(200, "text/html", DASHBOARD_HTML.encode())
        elif self.path.startswith("/summary.json"):
            self._respond(200, "application/json", self._data_json.encode())
        else:
            self._respond(404, "text/plain", b"Not found")

    def _respond(self, code: int, content_type: str, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def start_dashboard_server(port: int = 8090, polygon_coords=None) -> None:
    """Fetch all dashboard data, then serve the dashboard in the foreground (blocking)."""
    db = ListingsDB()
    try:
        data = {
            "overall": db.dashboard_overall_stats(),
            "categories": db.dashboard_category_breakdown(),
            "points": db.dashboard_sample_point_stats(),
            "zero_result_points": db.dashboard_zero_result_points(),
            "duplicate_hotspots": db.dashboard_duplicate_hotspots(),
            "duplicate_distribution": db.dashboard_duplicate_distribution(),
            "polygon": [],
        }
        breakdown = db.dashboard_point_category_breakdown()
        for point in data["points"]:
            point["categories"] = breakdown.get(point["id"], {})
        if polygon_coords:
            data["polygon"] = [[lat, lng] for lat, lng in polygon_coords]
    finally:
        db.close()

    data_json = json.dumps(data)

    from functools import partial
    handler = partial(_Handler, data_json)
    server = HTTPServer(("0.0.0.0", port), handler)

    url = f"http://localhost:{port}"
    logger.info(f"Dashboard: {url}")
    webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Dashboard server stopped")
    finally:
        server.server_close()
