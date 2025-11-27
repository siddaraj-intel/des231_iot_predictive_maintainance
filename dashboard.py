#!/usr/bin/env python3
"""
Clean RUL Dashboard 
Topic: topic_predictions_out
Format: {"MID":"100","timestamp":"2018-04-01 00:00:00","status_label":"normal","rul_pred":271.3041553891346}
"""

import os
import json
import time
import threading
from collections import deque
from datetime import datetime

from dash import Dash, dcc, html, Input, Output, dash_table
import plotly.graph_objs as go

try:
    from confluent_kafka import Consumer, KafkaError
except Exception as e:
    raise RuntimeError("Install: pip install confluent-kafka") from e

# Configuration - Updated Topic


BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "10.190.162.189:9092") # Hardcoded the IP as per the kakfa bootstrap .can alternatively use localhost
TOPIC = os.environ.get("KAFKA_TOPIC", "topic_predictions_out")  # Updated topic name
PORT = int(os.environ.get("PORT", "8055"))


# State
lock = threading.Lock()
machines = {}
history = deque(maxlen=1000)
stats = {"count": 0, "last_update": None}

def parse_message(msg_bytes):
    try:
        return json.loads(msg_bytes.decode("utf-8"))
    except:
        return None


"""
Determine equipment status based on Remaining Useful Life (RUL) value.
Status Threshold values:
    - Critical : RUL < 50         - Need Immediate attention
    - Warning  : 50 <= RUL < 100  - Maintenance Recommended
    - Normal   : RUL >= 100       - Equipment Operating Normally
"""
def get_status_from_rul(rul):
    if rul < 50:
        return "critical"
    elif rul < 100:
        return "warning"
    else:
        return "normal"

def kafka_consumer():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"rul-updated-{int(time.time())}",
        "auto.offset.reset": "latest"
    })
    consumer.subscribe([TOPIC])
    print(f"🔗 Subscribed to topic: {TOPIC}")
    
    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
                
            data = parse_message(msg.value())
            if not data:
                continue
            
            # Updated field mappings for new message format
            mid = data.get("MID")  # Changed from "machine_id" to "MID"
            rul = data.get("rul_pred")
            original_status = data.get("status_label", "normal")
            timestamp = data.get("timestamp")
            
            if mid and rul is not None:
                rul_float = float(rul)
                # Override status based on RUL value (keeping our logic)
                computed_status = get_status_from_rul(rul_float)
                
                with lock:
                    machines[mid] = {
                        "rul": rul_float,
                        "status": computed_status,
                        "original_status": original_status,  # Keep original for reference
                        "timestamp": timestamp
                    }
                    history.append((time.time(), mid, rul_float))
                    stats["count"] += 1
                    stats["last_update"] = datetime.now().strftime("%H:%M:%S")
                    
                # Debug print for first few messages
                if stats["count"] <= 5:
                    print(f"📨 Message {stats['count']}: MID={mid}, RUL={rul_float:.1f}, Status={computed_status}")
                    
        except Exception as e:
            print(f"❌ Kafka error: {e}")
            time.sleep(5)

# Start consumer
threading.Thread(target=kafka_consumer, daemon=True).start()

# Dashboard 
app = Dash(__name__)
app.title = "RUL Dashboard"

# Refresh internal of Dashboard is 3 Sec 
app.layout = html.Div([
    html.H1("🔧 RUL Fleet Monitor", style={
        "textAlign": "center", 
        "color": "#2c3e50", 
        "marginBottom": "20px"
    }),
    
    dcc.Interval(id="refresh", interval=3000, n_intervals=0),
    
    # Stats bar
    html.Div(id="stats", style={
        "background": "#34495e", 
        "color": "white", 
        "padding": "15px",
        "textAlign": "center", 
        "margin": "10px 0", 
        "borderRadius": "8px", 
        "fontSize": "16px",
        "fontWeight": "bold"
    }),
    
    html.Div([
        # Left: Charts
        html.Div([
            # Fixed Pie Chart
            html.Div([
                html.H3("Fleet Health Status", style={"color": "#2c3e50", "marginBottom": "15px"}),
                dcc.Graph(id="health-pie", style={"height": "400px"}),
            ], style={
                "background": "white", 
                "padding": "20px", 
                "borderRadius": "8px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                "marginBottom": "20px"
            }),
            
            # RUL Trend Lines
            html.Div([
                html.H3("Machine RUL Trends", style={"color": "#2c3e50", "marginBottom": "15px"}),
                dcc.Graph(id="rul-trends", style={"height": "400px"}),
            ], style={
                "background": "white", 
                "padding": "20px", 
                "borderRadius": "8px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)"
            }),
        ], style={"width": "48%", "display": "inline-block", "verticalAlign": "top"}),
        
        # Right: Table
        html.Div([
            html.Div([
                html.H3("Machine Status Details", style={"color": "#2c3e50", "marginBottom": "15px"}),
                
                dash_table.DataTable(
                    id="machine-table",
                    columns=[
                        {"name": "Machine ID", "id": "machine_id"},
                        {"name": "RUL Value", "id": "rul", "type": "numeric"},
                        {"name": "Status", "id": "status"},
                        {"name": "Last Update", "id": "timestamp"}
                    ],
                    style_cell={
                        "textAlign": "left", 
                        "padding": "12px",
                        "fontSize": "13px"
                    },
                    style_header={
                        "backgroundColor": "#34495e",
                        "color": "white",
                        "fontWeight": "bold"
                    },
                    style_data_conditional=[
                        {
                            "if": {"filter_query": "{status} = critical"},
                            "backgroundColor": "#e74c3c", 
                            "color": "white", 
                            "fontWeight": "bold"
                        },
                        {
                            "if": {"filter_query": "{status} = warning"},
                            "backgroundColor": "#f39c12", 
                            "color": "white"
                        },
                        {
                            "if": {"filter_query": "{status} = normal"},
                            "backgroundColor": "#27ae60", 
                            "color": "white"
                        }
                    ],
                    sort_action="native",
                    page_size=25,
                    style_table={"overflowX": "auto"}
                ),
                
                # Status Legend
                html.Div([
                    html.H4("Status Guide:", style={"color": "#2c3e50", "marginTop": "20px", "marginBottom": "10px"}),
                    html.Div("🔴 Critical: RUL < 50 (Immediate maintenance required)", 
                            style={"color": "#e74c3c", "fontWeight": "bold", "margin": "5px 0"}),
                    html.Div("🟡 Warning: 50 ≤ RUL < 100 (Schedule maintenance soon)", 
                            style={"color": "#f39c12", "fontWeight": "bold", "margin": "5px 0"}),
                    html.Div("🟢 Normal: RUL ≥ 100 (Operating normally)", 
                            style={"color": "#27ae60", "fontWeight": "bold", "margin": "5px 0"}),
                ], style={"fontSize": "14px"})
            ], style={
                "background": "white", 
                "padding": "20px", 
                "borderRadius": "8px",
                "boxShadow": "0 2px 4px rgba(0,0,0,0.1)",
                "height": "850px"
            })
        ], style={"width": "48%", "display": "inline-block", "verticalAlign": "top", "marginLeft": "4%"})
    ], style={"padding": "0 20px"})
], style={"background": "#f8f9fa", "minHeight": "100vh", "fontFamily": "Arial, sans-serif"})


@app.callback(Output("stats", "children"), Input("refresh", "n_intervals"))
def update_stats(_):
    with lock:
        total = len(machines)
        critical = sum(1 for m in machines.values() if m["status"] == "critical")
        warning = sum(1 for m in machines.values() if m["status"] == "warning")
        normal = sum(1 for m in machines.values() if m["status"] == "normal")
        count = stats["count"]
        last = stats["last_update"] or "Never"
    
    return f"📊 Total: {total} | 🚨 Critical: {critical} | ⚠️ Warning: {warning} | ✅ Normal: {normal} | 📈 Messages: {count} | ⏰ Last: {last}"

@app.callback(Output("health-pie", "figure"), Input("refresh", "n_intervals"))
def update_health_pie(_):
    with lock:
        statuses = [m["status"] for m in machines.values()]
    
    # Default: No data - white background with message
    if not statuses:
        fig = go.Figure()
        fig.add_annotation(
            text="No Data Available<br>Waiting for machine data...", 
            x=0.5, y=0.5, 
            showarrow=False,
            font=dict(size=16, color="#7f8c8d")
        )
        fig.update_layout(
            title="Fleet Health Status",
            paper_bgcolor="white",
            plot_bgcolor="white",
            margin=dict(l=40, r=40, t=60, b=40)
        )
        return fig
    
    # Count by status
    critical_count = statuses.count("critical")
    warning_count = statuses.count("warning")
    normal_count = statuses.count("normal")
    
    # Build pie chart data
    labels = []
    values = []
    colors = []
    
    if critical_count > 0:
        labels.append("🚨 Critical")
        values.append(critical_count)
        colors.append("#e74c3c")
    
    if warning_count > 0:
        labels.append("⚠️ Warning")
        values.append(warning_count)
        colors.append("#f39c12")
    
    if normal_count > 0:
        labels.append("✅ Normal")
        values.append(normal_count)
        colors.append("#27ae60")
    
    # Special case: All machines normal = full green pie
    if len(labels) == 1 and labels[0] == "✅ Normal":
        fig = go.Figure(data=[go.Pie(
            labels=["✅ All Systems Normal"],
            values=[normal_count],
            marker_colors=["#27ae60"],
            hole=0.4,
            textinfo='label+value',
            textfont=dict(size=14, color="white")
        )])
    else:
        # Mixed states
        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            marker_colors=colors,
            hole=0.3,
            textinfo='label+percent+value',
            textfont=dict(size=12)
        )])
    
    fig.update_layout(
        title="Fleet Health Distribution",
        paper_bgcolor="white",
        plot_bgcolor="white",
        showlegend=False,
        margin=dict(l=40, r=40, t=60, b=40)
    )
    return fig

@app.callback(Output("rul-trends", "figure"), Input("refresh", "n_intervals"))
def update_rul_trends(_):
    with lock:
        recent = list(history)[-100:]
        current_machines = dict(machines)
    
    if len(recent) < 5:
        fig = go.Figure()
        fig.add_annotation(
            text="Collecting trend data...<br>Need more data points", 
            x=0.5, y=0.5, 
            showarrow=False,
            font=dict(size=14, color="#7f8c8d")
        )
        fig.update_layout(
            title="Machine RUL Trends Over Time",
            paper_bgcolor="white",
            plot_bgcolor="white",
            margin=dict(l=40, r=40, t=60, b=40)
        )
        return fig
    
    # Group data by machine
    machine_data = {}
    for timestamp, machine_id, rul in recent:
        if machine_id not in machine_data:
            machine_data[machine_id] = {"times": [], "ruls": []}
        machine_data[machine_id]["times"].append(datetime.fromtimestamp(timestamp))
        machine_data[machine_id]["ruls"].append(rul)
    
    fig = go.Figure()
    
    # Add trend line for each machine (limit to top 10 most recent)
    machine_items = list(machine_data.items())
    for i, (machine_id, data) in enumerate(machine_items[:10]):
        # Color based on current status
        current_status = current_machines.get(machine_id, {}).get("status", "normal")
        if current_status == "critical":
            color = "#e74c3c"
        elif current_status == "warning":
            color = "#f39c12"
        else:
            color = "#27ae60"
        
        fig.add_trace(go.Scatter(
            x=data["times"],
            y=data["ruls"],
            mode='lines+markers',
            name=f"Machine {machine_id}",  # Added "Machine" prefix for clarity
            line=dict(color=color, width=2),
            marker=dict(size=4),
            hovertemplate=f'<b>Machine {machine_id}</b><br>RUL: %{{y:.1f}}<br>Time: %{{x}}<extra></extra>'
        ))
    
    # Add threshold lines
    fig.add_hline(y=50, line_dash="dash", line_color="#e74c3c", 
                  annotation_text="Critical Threshold (50)", annotation_position="bottom right")
    fig.add_hline(y=100, line_dash="dash", line_color="#f39c12", 
                  annotation_text="Warning Threshold (100)", annotation_position="top right")
    
    fig.update_layout(
        title="Machine RUL Trends (Last 10 Machines)",
        xaxis_title="Time",
        yaxis_title="RUL Value",
        paper_bgcolor="white",
        plot_bgcolor="white",
        legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02),
        margin=dict(l=40, r=40, t=60, b=40)
    )
    return fig

@app.callback(Output("machine-table", "data"), Input("refresh", "n_intervals"))
def update_table(_):
    with lock:
        data = []
        for mid, info in machines.items():
            data.append({
                "machine_id": f"Machine {mid}",  # Added "Machine" prefix for display
                "rul": round(info["rul"], 2),
                "status": info["status"],
                "timestamp": info["timestamp"] or "Unknown"
            })
    
    # Sort by RUL (critical first)
    return sorted(data, key=lambda x: x["rul"])

if __name__ == "__main__":
    print(f"🚀 Updated RUL Dashboard on port {PORT}")
    print(f"📡 Topic: {TOPIC}")
    print(f"📋 Message Format: MID, timestamp, status_label, rul_pred")
    print(f"📊 Status Logic: Critical (RUL < 50), Warning (50-100), Normal (≥100)")
    print(f"🔗 Access: http://localhost:{PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)
