# Meta-Crawler Kern mit Logging, Zeitplan, Live-Übersicht, KI-Modell und Deployment-Start
from flask import Flask, request, render_template_string, redirect, url_for, session, jsonify, Response
import os
import logging
import time
import json
import csv
import requests
import sqlite3
import schedule
import threading
from datetime import datetime, timedelta
import folium
from folium.plugins import HeatMap

# App-Instanz
app = Flask(__name__)
app.secret_key = 'terrasignum_secret'

# Datenbank- und Pfad-Konfiguration
db_path = 'terrasignum_data.db'
DB_NAME = db_path
STATIC = 'static'
CRAWL_LOG_PATH = os.path.join(STATIC, 'crawl_schedule.csv')
# Verzeichnisse und Log-Datei sicherstellen
os.makedirs(STATIC, exist_ok=True)
if not os.path.exists(CRAWL_LOG_PATH):
    with open(CRAWL_LOG_PATH, 'w', newline='') as f:
        csv.writer(f).writerow(["project_id", "source", "last_run", "status"])

# Logging-Setup
logging.basicConfig(filename='meta_crawler.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

meta_sources = {
    "USGS": {
        "type": "json",
        "url": "https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&limit=10",
        "parser": "usgs_parser"
    },
    "OpenMeteo": {
        "type": "weather",
        "url_template": "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m&timezone=UTC",
        "parser": "openmeteo_parser"
    },
    "NASA-FIRMS": {
        "type": "csv",
        "url": "https://firms.modaps.eosdis.nasa.gov/data/active_fire/viirs/csv/MODIS_C6_USA_contiguous_and_Hawaii_24h.csv",
        "parser": "nasa_firms_parser"
    },
    "DAI-SPARQL": {
        "type": "sparql",
        "url": "https://gazetteer.dainst.org/sparql",
        "parser": "dai_sparql_parser"
    }
}

# Logging-Setup
logging.basicConfig(filename='meta_crawler.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Nutzerverwaltung vorbereiten
@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        user = request.form.get("username")
        pw = request.form.get("password")
        with sqlite3.connect(DB_NAME) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, role TEXT)")
            row = conn.execute("SELECT * FROM users WHERE username=? AND password=?", (user, pw)).fetchone()
        if row:
            session["user"] = user
            return redirect(url_for("crawler_dashboard"))
        error = "Zugangsdaten falsch"
    return render_template_string('''<h2>Login</h2>
        <form method="post">
        Benutzer: <input name="username"><br>
        Passwort: <input type="password" name="password"><br>
        <input type="submit" value="Login">
        {% if error %}<p style="color:red">{{error}}</p>{% endif %}
        </form>''', error=error)

@app.route("/logout")
def logout():
    session.pop("user", None)
    return redirect("/login")

# Quellen-Webinterface pro Projekt
@app.route("/project/<project_id>/sources", methods=["GET", "POST"])
def project_source_toggle(project_id):
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS project_sources (
    project_id TEXT,
    source TEXT,
    active INTEGER DEFAULT 1,
    priority INTEGER DEFAULT 0,
    interval_seconds INTEGER DEFAULT 300,
    last_run TEXT,
    backoff_until TEXT
)''')
        conn.commit()
        if request.method == "POST":
            for source in meta_sources.keys():
                active = 1 if request.form.get(source) == "on" else 0
                conn.execute("REPLACE INTO project_sources (project_id, source, active) VALUES (?, ?, ?)", (project_id, source, active))
            conn.commit()
        sources = conn.execute("SELECT source, active FROM project_sources WHERE project_id=?", (project_id,)).fetchall()
    return render_template_string('''
        <h2>Quellensteuerung für Projekt {{ project_id }}</h2>
        <form method="post">
        {% for s, active in sources %}
            <input type="checkbox" name="{{ s }}" {% if active %}checked{% endif %}> {{ s }}<br>
        {% endfor %}
        <input type="submit" value="Speichern">
        </form>
        <a href="/crawler/dashboard">Zurück</a>
    ''', project_id=project_id, sources=sources)

# Relevanz-Zeitreihe als Chart.js
@app.route("/crawler/relevance_chart_data/<project_id>")
def relevance_chart_data(project_id):
    scores = {}
    if os.path.exists(CRAWL_LOG_PATH):
        with open(CRAWL_LOG_PATH, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['project_id'] != project_id or row['status'] not in ('ok', 'fail', 'error'):
                    continue
                ts = row['last_run'][:16]  # kürzen auf Minute
                key = (row['source'], ts)
                scores.setdefault(key, {'ok': 0, 'fail': 0, 'error': 0})
                scores[key][row['status']] += 1

    timeline = {}
    for (source, ts), val in scores.items():
        total = val['ok'] + val['fail'] + val['error']
        if total == 0: continue
        success = val['ok'] / total * 100
        timeline.setdefault(source, []).append((ts, success))

    return jsonify(timeline)

@app.route("/crawler/relevance_chart/<project_id>")
def relevance_chart(project_id):
    scores = {}
    if os.path.exists(CRAWL_LOG_PATH):
        with open(CRAWL_LOG_PATH, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['project_id'] != project_id or row['status'] not in ('ok', 'fail', 'error'):
                    continue
                ts = row['last_run'][:16]  # kürzen auf Minute
                key = (row['source'], ts)
                scores.setdefault(key, {'ok': 0, 'fail': 0, 'error': 0})
                scores[key][row['status']] += 1

    timeline = {}
    for (source, ts), val in scores.items():
        total = val['ok'] + val['fail'] + val['error']
        if total == 0: continue
        success = val['ok'] / total * 100
        timeline.setdefault(source, []).append((ts, success))

    return render_template_string('''
        <h2>Relevanz-Zeitverlauf für Projekt {{project_id}}</h2>
        <div style="width:100%;max-width:900px">
        <canvas id="chart"></canvas>
        </div>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script>
        const data = {
            labels: {{ timeline.values()|map(attribute=0)|list|first|map(attribute=0)|list }},
            datasets: [
            {% for src, pairs in timeline.items() %}
            {
                label: '{{src}}',
                data: [ {% for p in pairs %} {{p[1]}}, {% endfor %} ],
                fill: false,
                borderColor: '{{ ['red', 'blue', 'green', 'orange', 'purple', 'teal', 'black'][loop.index0 % 7] }}',
                tension: 0.1
            },
            {% endfor %}
            ]
        };
        let chart;
        fetch('/crawler/relevance_chart_data/{{project_id}}')
            .then(res => res.json())
            .then(data => {
                const sources = Object.keys(data);
                const labels = data[sources[0]].map(e => e[0]);
                const datasets = sources.map((s, i) => ({
                    label: s,
                    data: data[s].map(e => e[1]),
                    borderColor: ['red', 'blue', 'green', 'orange', 'purple', 'teal', 'black'][i % 7],
                    fill: false,
                    tension: 0.1
                }));
                chart = new Chart(document.getElementById('chart'), {
                    type: 'line',
                    data: { labels: labels, datasets: datasets },
            type: 'line', data: data,
            options: {
                        scales: { y: { beginAtZero: true, max: 100 } },
                        animation: false,
                        responsive: true
                    }
                });
                setInterval(() => {
                    fetch('/crawler/relevance_chart_data/{{project_id}}')
                        .then(res => res.json())
                        .then(update => {
                            chart.data.labels = update[sources[0]].map(e => e[0]);
                            chart.data.datasets.forEach((ds, i) => {
                                const s = sources[i];
                                ds.data = update[s].map(e => e[1]);
                            });
                            chart.update();
                        });
                }, 15000);
        });
        </script>
        <a href="/crawler/dashboard">Zurück</a>
    ''', timeline=timeline, project_id=project_id)

# Heatmap der Crawls pro Projekt/Quelle
@app.route("/crawler/heatmap/<project_id>")
def crawler_heatmap(project_id):
    points = []
    with sqlite3.connect(DB_NAME) as conn:
        rows = conn.execute("SELECT latitude, longitude FROM project_entries WHERE project_id=?", (project_id,)).fetchall()
        for lat, lon in rows:
            if lat and lon:
                points.append([lat, lon])

    m = folium.Map(location=[points[0][0], points[0][1]] if points else [0, 0], zoom_start=3)
    from folium.plugins import HeatMap
    HeatMap(points).add_to(m)
    path = os.path.join(STATIC, f"heatmap_{project_id}.html")
    m.save(path)
    return redirect("/" + path)

# Fehlertrend-Visualisierung als Chart.js
@app.route("/crawler/error_trend/<project_id>")
def error_trend_chart(project_id):
    counts = {}
    if os.path.exists(CRAWL_LOG_PATH):
        with open(CRAWL_LOG_PATH, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['project_id'] != project_id:
                    continue
                date = row['last_run'][:10]
                status = row['status']
                counts.setdefault(date, {'ok': 0, 'fail': 0, 'error': 0})
                if status in counts[date]:
                    counts[date][status] += 1

    labels = list(counts.keys())
    ok = [counts[k]['ok'] for k in labels]
    fail = [counts[k]['fail'] for k in labels]
    error = [counts[k]['error'] for k in labels]

    return render_template_string('''
        <h2>Fehlertrend für Projekt {{project_id}}</h2>
        <canvas id="trend" width="900" height="400"></canvas>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <script>
        new Chart(document.getElementById('trend'), {
            type: 'line',
            data: {
                labels: {{ labels|tojson }},
                datasets: [
                    { label: 'OK', data: {{ ok|tojson }}, borderColor: 'green', fill: false },
                    { label: 'Fail', data: {{ fail|tojson }}, borderColor: 'orange', fill: false },
                    { label: 'Error', data: {{ error|tojson }}, borderColor: 'red', fill: false }
                ]
            },
            options: {
                scales: { y: { beginAtZero: true } }
            }
        });
        </script>
        <a href="/crawler/dashboard">Zurück</a>
    ''', project_id=project_id, labels=labels, ok=ok, fail=fail, error=error)

# System-Doku anzeigen
@app.route("/system/info")
def system_info():
    return render_template_string('''
    <h2>TerraCrawler Systeminfo</h2>
    <ul>
        <li><a href="/crawler/dashboard">Dashboard</a></li>
        <li><a href="/project/testproj/sources">Quellenauswahl</a></li>
        <li><a href="/crawler/errors">Fehlerübersicht</a></li>
        <li><a href="/crawler/relevance_chart/testproj">Relevanztrend (Chart)</a></li>
        <li><a href="/crawler/heatmap/testproj">Heatmap</a></li>
        <li><a href="/crawler/live_export/testproj.csv">Live-Export</a></li>
        <li><a href="/login">Login</a></li>
    </ul>
    ''')

# Crawl-Tabelle vorbereiten + Live-Übersicht und Dashboard bereitstellen
from flask import jsonify, request

@app.route("/crawler/dashboard")
def crawler_dashboard():
    log_data = []
    cleanup_stats = []
    if os.path.exists(CRAWL_LOG_PATH):
        with open(CRAWL_LOG_PATH, newline='') as f:
            reader = csv.DictReader(f)
            log_data = list(reader)[-100:]
    # Lade alle Projekte und zähle Fundstellen
    with sqlite3.connect(DB_NAME) as conn:
        stats = conn.execute("""
            SELECT project_id, source, COUNT(*) FROM project_entries
            GROUP BY project_id, source
        """).fetchall()
        for row in stats:
            cleanup_stats.append({"project_id": row[0], "source": row[1], "count": row[2]})
                # Relevanzberechnung vorbereiten
        crawl_counts = {}
        for log in log_data:
            key = (log['project_id'], log['source'])
            crawl_counts.setdefault(key, {'ok': 0, 'fail': 0, 'error': 0, 'total': 0})
            crawl_counts[key][log['status']] += 1
            crawl_counts[key]['total'] += 1

        for c in cleanup_stats:
            key = (c['project_id'], c['source'])
            stats = crawl_counts.get(key, {'ok': 0, 'fail': 0, 'error': 0, 'total': 1})
            success = stats['ok']
            total = stats['total']
            score = round(100 * success / total) if total > 0 else 0
            c['relevance'] = score
            c['recommend'] = "👍 Empfohlen" if score >= 70 else ("⚠️ Mittel" if score >= 30 else "🚫 Vermeiden")

        return render_template_string('''
        <h2 style="font-family:sans-serif; margin-bottom: 1em">Meta-Crawler Dashboard</h2>
        <div style="margin-bottom:1em">
            <a href="/crawler/errors">Fehlerübersicht</a> |
            <a href="/project/{{ cleanup[0].project_id }}/sources">Quellenauswahl</a> |
            <a href="/crawler/heatmap/{{ cleanup[0].project_id }}">Heatmap</a> |
            <a href="/crawler/error_trend/{{ cleanup[0].project_id }}">Fehlertrend</a> |
            <a href="/crawler/relevance_chart/{{ cleanup[0].project_id }}">Relevanz-Chart</a> |
            <a href="/crawler/live_export/{{ cleanup[0].project_id }}.csv">CSV Export</a>
        </div>
        <style>
            table { border-collapse: collapse; margin-bottom: 2em; width: 100%; font-family: sans-serif; overflow-x: auto; display: block; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: center; white-space: nowrap; }
            th { background-color: #f2f2f2; font-weight: bold; }
            tr:hover { background-color: #f9f9f9; }
            .score-bar {
                width: 100%; height: 10px; border-radius: 4px;
                background: linear-gradient(to right, green, yellow, red);
            }
            .score-fill {
                height: 10px; border-radius: 4px;
                background-color: #4CAF50;
            }
        </style>
        <h3>Letzte Crawls</h3>
        <table border=1 id="log-table"><tr><th>Projekt</th><th>Quelle</th><th>Zeit</th><th>Status</th><th>⏱ Trigger</th></tr>
        {% for l in logs %}<tr><td>{{l.project_id}}</td><td>{{l.source}}</td><td>{{l.last_run}}</td><td>{{l.status}}</td><td>{% if l.trigger_type == 'manual' %}<span style='color:orange'>🖱 Manuell</span>{% else %}Auto{% endif %}</td></tr>{% endfor %}</table>
        <script>
        setInterval(() => {
            document.querySelectorAll('td span[data-countdown]').forEach(el => {
                let sec = parseInt(el.dataset.countdown);
                if (!isNaN(sec) && sec > 0) {
                    sec -= 1;
                    el.dataset.countdown = sec;
                    el.textContent = sec + 's';
                    el.style.color = sec < 30 ? 'green' : (sec < 120 ? 'orange' : 'red');
                }
            });
        }, 1000);
        setInterval(() => {
            fetch('/crawler/logs.json').then(res => res.json()).then(data => {
                const tbody = document.getElementById('log-table');
                if (!tbody) return;
                let html = '<tr><th>Projekt</th><th>Quelle</th><th>Zeit</th><th>Status</th><th>⏱ Trigger</th></tr>';
                for (let row of data) {
                    html += `<tr><td>${row.project_id}</td><td>${row.source}</td><td>${row.last_run}</td><td>${row.status}</td><td>${row.trigger_type === 'manual' ? '🖱 Manuell' : 'Auto'}</td></tr>`;
                }
                tbody.innerHTML = html;
            });
        }, 10000);
        </script>
        <h3>Top 3 Quellen nach Relevanz</h3>
        <ul>
        {% for top in cleanup|sort(attribute='relevance', reverse=True)[:3] %}
            <li><b>{{ top.source }}</b> – {{ top.relevance }}%</li>
        {% endfor %}
        </ul>
        <h3>Fundstellen nach Quelle</h3>
        <table border=1><tr><th>Projekt</th><th>Quelle</th><th>Einträge</th><th>⚠️ Hinweis</th><th>🔢 Relevanz</th><th>🤖 Empfehlung</th><th>⏳ Countdown</th></tr>
        {% for c in cleanup %}
        <tr>
            <td>{{c.project_id}}</td>
            <td>
                {% if c.source %}
                    <img src="/static/icons/{{c.source}}.png" alt="{{c.source}}" width="20" style="vertical-align:middle"> {{c.source}}
                {% else %}
                    {{c.source}}
                {% endif %}
            </td>
            <td>{{c.count}}</td>
            <td>{% if c.count|int > 1000 %}<b style='color:red'>Zu viele!</b>{% else %}OK{% endif %}</td>
            <td>
                <div style='position:relative; height:12px;'>
                    <div class='score-bar'>
                        <div class='score-fill' style='width: {{c.relevance}}%; background-color: {% if c.relevance >= 70 %}green{% elif c.relevance >= 30 %}orange{% else %}red{% endif %};'></div>
                    </div>
                    <div style='position:absolute; top:-18px; width:100%; font-size:10px;'>{{c.relevance}}%</div>
                </div>
            </td>
            <td>{{c.recommend}}</td><td><form method='post' action='/crawler/manual_run/{{c.project_id}}/{{c.source}}'><button>⏯ Jetzt crawlen</button></form></td><td>{% if c.countdown is defined %}
                <span data-countdown="{{ c.countdown|int }}" style='color:{% if c.countdown|int < 30 %}green{% elif c.countdown|int < 120 %}orange{% else %}red{% endif %}'>
                    {{ c.countdown }}s
                </span>
            {% else %}–{% endif %}</td>
        </tr>
        {% endfor %}</table>
    ''', logs=log_data, cleanup=cleanup_stats)

@app.route("/crawler/manual_run/<project_id>/<source>", methods=["POST"])
def manual_run(project_id, source):
    logging.info(f"Manueller Crawl ausgelöst: {project_id}/{source}")
    try:
        meta_crawler_run(project_id, override_source=source)
        return redirect(url_for("crawler_dashboard"))
    except Exception as e:
        return f"Fehler beim manuellen Crawlen: {e}"

@app.route("/crawler/logs.json")
def crawler_logs_json():
    logs = []
    if os.path.exists(CRAWL_LOG_PATH):
        with open(CRAWL_LOG_PATH, newline='') as f:
            reader = csv.DictReader(f)
            logs = list(reader)[-50:]
    return jsonify(logs)

@app.route("/crawler/export/<project_id>.json")
def crawler_export_json(project_id):
    with sqlite3.connect(DB_NAME) as conn:
        rows = conn.execute("SELECT * FROM project_entries WHERE project_id=?", (project_id,)).fetchall()
        cols = [d[0] for d in conn.execute("PRAGMA table_info(project_entries)")]
        return jsonify([dict(zip(cols, row)) for row in rows])

@app.route("/crawler/live_export/<project_id>.csv")
def live_export_csv(project_id):
    import io
    output = io.StringIO()
    writer = csv.writer(output)
    with sqlite3.connect(DB_NAME) as conn:
        rows = conn.execute("SELECT * FROM crawl_log WHERE project_id=? ORDER BY last_run DESC LIMIT 500", (project_id,)).fetchall()
        cols = [d[0] for d in conn.execute("PRAGMA table_info(crawl_log)")]
        writer.writerow(cols)
        writer.writerows(rows)
    output.seek(0)
    from flask import Response
    return Response(output, mimetype='text/csv', headers={'Content-Disposition': f'attachment;filename=live_export_{project_id}.csv'})

@app.route("/crawler/status")
def crawler_status():
    logs = []
    if os.path.exists(CRAWL_LOG_PATH):
        with open(CRAWL_LOG_PATH, newline='') as f:
            reader = csv.DictReader(f)
            logs = list(reader)[-50:]  # letzte 50 Einträge
    return jsonify(logs)

@app.route("/crawler/errors")
def crawler_errors():
    errors = []
    if os.path.exists(CRAWL_LOG_PATH):
        with open(CRAWL_LOG_PATH, newline='') as f:
            reader = csv.DictReader(f)
            errors = [row for row in reader if row['status'] in ('fail', 'error')]
    return render_template_string('''
        <h2>Fehlerübersicht Meta-Crawler</h2>
        <table border=1><tr><th>Projekt</th><th>Quelle</th><th>Zeit</th><th>Status</th></tr>
        {% for e in errors %}
        <tr style="color:red">
            <td>{{e.project_id}}</td>
            <td>{{e.source}}</td>
            <td>{{e.last_run}}</td>
            <td>{{e.status}}</td>
        </tr>
        {% endfor %}</table>
        <a href="/crawler/dashboard">Zurück zum Dashboard</a>
    ''', errors=errors)
CRAWL_LOG_PATH = 'static/crawl_schedule.csv'
os.makedirs('static', exist_ok=True)
if not os.path.exists(CRAWL_LOG_PATH):
    with open(CRAWL_LOG_PATH, 'w', newline='') as f:
        csv.writer(f).writerow(["project_id", "source", "last_run", "status"])

import smtplib
from email.message import EmailMessage

ADMIN_EMAIL = "admin@example.com"

def send_alert_email(project_id, source, status):
    msg = EmailMessage()
    msg.set_content(f"Achtung: Crawl-Fehler für Projekt {project_id} bei Quelle {source} – Status: {status}")
    msg['Subject'] = f'Crawl-Fehler in TerraSignum: {source} ({project_id})'
    msg['From'] = 'crawler@terrasignum.com'
    msg['To'] = ADMIN_EMAIL
    try:
        with smtplib.SMTP('localhost') as s:
            s.send_message(msg)
    except Exception as e:
        logging.error(f"E-Mail-Fehler: {e}")

def log_crawl(project_id, source, status, trigger_type="auto"):
    # SQLite-Log zusätzlich
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS crawl_log (
                            project_id TEXT,
                            source TEXT,
                            last_run TEXT,
                            status TEXT,
                            trigger_type TEXT
                        )''')
        conn.execute("INSERT INTO crawl_log VALUES (?, ?, ?, ?, ?)", (project_id, source, datetime.utcnow().isoformat(), status, trigger_type))
        conn.commit()
    if os.path.exists(CRAWL_LOG_PATH):
        with open(CRAWL_LOG_PATH, newline='') as f:
            rows = list(csv.reader(f))[-999:]
    else:
        rows = [["project_id", "source", "last_run", "status", "trigger_type"]]
    rows.append([project_id, source, datetime.utcnow().isoformat(), status, trigger_type])
    with open(CRAWL_LOG_PATH, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)

def get_active_sources(project_id):
    now = datetime.utcnow().isoformat()

    with sqlite3.connect(DB_NAME) as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS project_sources (
                            project_id TEXT,
                            source TEXT,
                            active INTEGER DEFAULT 1,
                            priority INTEGER DEFAULT 0
                        )''')
        conn.commit()
        rows = conn.execute("""
            SELECT source FROM project_sources
            WHERE project_id=? AND active=1 AND (backoff_until IS NULL OR backoff_until < ?)
        """, (project_id, now)).fetchall()
        return [r[0] for r in rows] if rows else list(meta_sources.keys())

def update_backoff(project_id, source, minutes=10):
    until = (datetime.utcnow() + timedelta(minutes=minutes)).isoformat()
    with sqlite3.connect(DB_NAME) as conn:
        conn.execute("UPDATE project_sources SET backoff_until=? WHERE project_id=? AND source=?", (until, project_id, source))
        conn.commit()

def meta_crawler_run(project_id, override_source=None):
    meta_crawler_cleanup(project_id)

    # KI-Relevanzbewertung pro Quelle abrufen und sortieren
    crawl_scores = {}
    if os.path.exists(CRAWL_LOG_PATH):
        with open(CRAWL_LOG_PATH, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['project_id'] != project_id:
                    continue
                key = row['source']
                crawl_scores.setdefault(key, {'ok': 0, 'fail': 0, 'error': 0})
                crawl_scores[key][row['status']] += 1

    relevance_order = []
    for source, stats in crawl_scores.items():
        total = stats['ok'] + stats['fail'] + stats['error']
        score = 100 * stats['ok'] / total if total else 0
        relevance_order.append((source, score))

    relevance_order.sort(key=lambda x: x[1], reverse=True)  # höchste Relevanz zuerst

    sorted_sources = [s for s, _ in relevance_order] if relevance_order else list(meta_sources.keys())
    logging.info(f"Starte Meta-Crawler für Projekt {project_id}")
    active_sources = get_active_sources(project_id)
    for name in sorted_sources:
        if override_source and name != override_source:
            continue
        config = meta_sources.get(name)
        if not config:
            continue
        if name not in active_sources:
            logging.info(f"Quelle {name} für Projekt {project_id} deaktiviert – übersprungen.")
            continue
        try:
            status = "ok"
            if config["type"] == "json":
                response = requests.get(config["url"])
                if response.status_code == 200:
                    if config["parser"] == "usgs_parser":
                        usgs_parser(response.json(), project_id)
            elif config["type"] == "csv":
                response = requests.get(config["url"])
                if response.status_code != 200:
                    status = "fail"
            elif config["type"] == "sparql":
                headers = {"Accept": "application/sparql-results+json"}
                r = requests.post(config["url"], data={"query": "SELECT ?name ?lat ?lon WHERE {?place rdfs:label ?name ; geo:lat ?lat ; geo:long ?lon } LIMIT 5"}, headers=headers)
                if r.status_code != 200:
                    status = "fail"
            elif config["type"] == "weather":
                center = get_project_center(project_id)
                if center:
                    lat, lon = center
                    url = config["url_template"].format(lat=lat, lon=lon)
                    response = requests.get(url)
                    if response.status_code != 200:
                        status = "fail"
            log_crawl(project_id, name, status)
            if status in ["fail", "error"]:
                update_backoff(project_id, name, 10)
            if status in ["fail", "error"]:
                send_alert_email(project_id, name, status)
        except Exception as e:
            log_crawl(project_id, name, "error")
            logging.error(f"Fehler bei Quelle {name}: {e}")

def start_meta_crawler_scheduler(project_id):
    def schedule_project_sources():
        with sqlite3.connect(DB_NAME) as conn:
            now = datetime.utcnow()
            entries = conn.execute("""
                SELECT ps.project_id, ps.source, ps.interval_seconds, MAX(cl.last_run)
                FROM project_sources ps
                LEFT JOIN (
                    SELECT * FROM (
                        SELECT * FROM (
                            SELECT * FROM (SELECT * FROM (
                                SELECT * FROM (SELECT * FROM (
                                    SELECT * FROM project_entries) ) ) ) )
                    )
                ) cl
                ON ps.project_id = cl.project_id AND ps.source = cl.source
                WHERE ps.active = 1
            """).fetchall()

            for pid, source, interval, last_run in entries:
                if last_run:
                    last = datetime.fromisoformat(last_run)
                    delta = (now - last).total_seconds()
                    if delta < interval:
                        continue  # noch nicht fällig
                logging.info(f"Geplanter Crawl für {pid}/{source}")
                meta_crawler_run(pid)

    schedule.every(30).seconds.do(schedule_project_sources)
    logging.info(f"Scheduler gestartet für Projekt {project_id} alle 30 Sekunden.")
    def run_scheduler():
        logging.info("Meta-Crawler-Scheduler läuft...")
        while True:
            schedule.run_pending()
            time.sleep(10)
    threading.Thread(target=run_scheduler, daemon=True).start()

def start_all_project_schedulers(interval_minutes=5):
    with sqlite3.connect("terrasignum_data.db") as conn:
        project_ids = [row[0] for row in conn.execute("SELECT DISTINCT project_id FROM project_entries")]
    for pid in project_ids:
        start_meta_crawler_scheduler(pid)
    logging.info(f"Scheduler für alle {len(project_ids)} Projekte gestartet.")

# Parser-Beispiel
def usgs_parser(data, project_id):
    features = data.get("features", [])
    with sqlite3.connect("terrasignum_data.db") as conn:
        for f in features:
            coords = f.get("geometry", {}).get("coordinates", [None, None])
            props = f.get("properties", {})
            if coords and props:
                lon, lat = coords[0], coords[1]
                comment = props.get("title", "USGS Event")
                conn.execute("INSERT INTO project_entries (project_id, source, latitude, longitude, comment) VALUES (?, ?, ?, ?, ?)",
                             (project_id, "USGS", lat, lon, comment))
        conn.commit()

# Autonomes Fehlerüberwachungsmodul

def meta_crawler_cleanup(project_id):
    logging.info(f"Starte Cleanup für Projekt {project_id}")
    with sqlite3.connect(DB_NAME) as conn:
        c = conn.cursor()
        # Doppelte Einträge nach Quelle, Koordinaten, Kommentar
        c.execute('''DELETE FROM project_entries
                     WHERE rowid NOT IN (
                         SELECT MIN(rowid) FROM project_entries
                         WHERE project_id = ?
                         GROUP BY source, latitude, longitude, comment
                     ) AND project_id = ?''', (project_id, project_id))
        logging.info(f"Duplikate bereinigt in Projekt {project_id}")

        # Leere Koordinaten oder sinnlose Einträge löschen
        c.execute('''DELETE FROM project_entries
                     WHERE (latitude IS NULL OR longitude IS NULL OR latitude = 0 OR longitude = 0)
                     AND project_id = ?''', (project_id,))
        logging.info(f"Leere oder ungültige Daten entfernt in Projekt {project_id}")

        # Irrelevante Kommentare (z. B. nur Zahlen oder kürzer als 3 Zeichen)
        c.execute('''DELETE FROM project_entries
                     WHERE LENGTH(comment) < 3 AND project_id = ?''', (project_id,))
        conn.commit()
        logging.info(f"Kurzkommentare entfernt in Projekt {project_id}")

# Projektzentrum ermitteln
def get_project_center(project_id):
    with sqlite3.connect("terrasignum_data.db") as conn:
        row = conn.execute("SELECT AVG(latitude), AVG(longitude) FROM project_entries WHERE project_id=?", (project_id,)).fetchone()
    return row if row and row[0] and row[1] else None
