"""Background HTTP server serving a live Leaflet.js scraping dashboard."""

import logging
import threading
from functools import partial
from http.server import HTTPServer, BaseHTTPRequestHandler

from scraper.progress import ProgressTracker

logger = logging.getLogger(__name__)

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Scraper Live Map</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  #map { position: absolute; top: 0; left: 0; right: 0; bottom: 0; }
  #stats {
    position: absolute; top: 12px; right: 12px; z-index: 1000;
    background: rgba(255,255,255,0.95); border-radius: 8px;
    padding: 14px 18px; box-shadow: 0 2px 8px rgba(0,0,0,0.2);
    font-size: 14px; line-height: 1.7; min-width: 220px;
  }
  #stats h3 { margin-bottom: 6px; font-size: 15px; }
  .stat-row { display: flex; justify-content: space-between; }
  .stat-label { color: #555; }
  .stat-value { font-weight: 600; }
  .legend { margin-top: 10px; border-top: 1px solid #eee; padding-top: 8px; }
  .legend-item { display: flex; align-items: center; gap: 6px; font-size: 12px; margin: 2px 0; }
  .legend-dot { width: 12px; height: 12px; border-radius: 50%; display: inline-block; }
  .dot-pending { background: #9e9e9e; }
  .dot-active { background: #ff9800; }
  .dot-done { background: #4caf50; }
  #progress-bar { width: 100%; height: 6px; background: #eee; border-radius: 3px; margin-top: 8px; }
  #progress-fill { height: 100%; background: #4caf50; border-radius: 3px; transition: width 0.5s; }
</style>
</head>
<body>
<div id="map"></div>
<div id="stats">
  <h3>Scraper Progress</h3>
  <div class="stat-row"><span class="stat-label">Area</span><span class="stat-value" id="s-area">--</span></div>
  <div class="stat-row"><span class="stat-label">Points</span><span class="stat-value" id="s-points">--</span></div>
  <div class="stat-row"><span class="stat-label">Businesses</span><span class="stat-value" id="s-biz">--</span></div>
  <div class="stat-row"><span class="stat-label">Elapsed</span><span class="stat-value" id="s-time">--</span></div>
  <div id="progress-bar"><div id="progress-fill" style="width:0%"></div></div>
  <div class="legend">
    <div class="legend-item"><span class="legend-dot dot-pending"></span> Pending</div>
    <div class="legend-item"><span class="legend-dot dot-active"></span> Active</div>
    <div class="legend-item"><span class="legend-dot dot-done"></span> Done</div>
  </div>
</div>
<script>
const map = L.map('map');
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '&copy; OpenStreetMap'
}).addTo(map);

let polygonLayer = null;
let pointLayers = [];
let initialized = false;

const COLORS = { pending: '#9e9e9e', active: '#ff9800', done: '#4caf50' };

function formatTime(sec) {
  const m = Math.floor(sec / 60);
  const s = Math.floor(sec % 60);
  return m > 0 ? m + 'm ' + s + 's' : s + 's';
}

async function poll() {
  try {
    const res = await fetch('/progress.json?' + Date.now());
    const d = await res.json();

    // Draw polygon once
    if (!initialized && d.polygon.length > 0) {
      polygonLayer = L.polygon(d.polygon, {
        color: '#1976d2', weight: 2, fillOpacity: 0.08
      }).addTo(map);
      map.fitBounds(polygonLayer.getBounds().pad(0.1));
      initialized = true;
    }

    // Update points
    pointLayers.forEach(l => map.removeLayer(l));
    pointLayers = [];
    d.points.forEach((p, i) => {
      const color = COLORS[p.status];
      const circle = L.circleMarker([p.lat, p.lng], {
        radius: p.status === 'active' ? 10 : 8,
        color: color,
        fillColor: color,
        fillOpacity: 0.8,
        weight: p.status === 'active' ? 3 : 1,
      }).addTo(map);
      circle.bindTooltip(
        'Point ' + (i+1) + ': ' + p.status + '<br>' + p.businesses + ' businesses',
        { direction: 'top' }
      );
      // Show count label for done points
      if (p.businesses > 0) {
        const label = L.marker([p.lat, p.lng], {
          icon: L.divIcon({
            className: '',
            html: '<div style="color:#fff;font-size:10px;font-weight:700;text-align:center;margin-top:-5px;">' + p.businesses + '</div>',
            iconSize: [20, 20],
          })
        }).addTo(map);
        pointLayers.push(label);
      }
      pointLayers.push(circle);
    });

    // Update stats
    document.getElementById('s-area').textContent = d.area_km2 + ' km\u00B2';
    document.getElementById('s-points').textContent = d.done_points + ' / ' + d.total_points;
    document.getElementById('s-biz').textContent = d.total_businesses;
    document.getElementById('s-time').textContent = formatTime(d.elapsed_seconds);
    const pct = d.total_points > 0 ? (d.done_points / d.total_points * 100) : 0;
    document.getElementById('progress-fill').style.width = pct + '%';
  } catch (e) {}
}

poll();
setInterval(poll, 2000);
</script>
</body>
</html>"""


class _Handler(BaseHTTPRequestHandler):
    """Serves dashboard HTML and progress JSON."""

    def __init__(self, tracker: ProgressTracker, *args, **kwargs):
        self.tracker = tracker
        super().__init__(*args, **kwargs)

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self._respond(200, "text/html", DASHBOARD_HTML.encode())
        elif self.path.startswith("/progress.json"):
            self._respond(200, "application/json", self.tracker.to_json().encode())
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
        # Suppress default request logging
        pass


def start_live_server(tracker: ProgressTracker, port: int = 8080) -> HTTPServer:
    """Start the dashboard server in a background daemon thread. Returns the server."""
    handler = partial(_Handler, tracker)
    server = HTTPServer(("0.0.0.0", port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Live map: http://localhost:{port}")
    return server
