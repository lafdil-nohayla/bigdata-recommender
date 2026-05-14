"""
Flask Dashboard API
───────────────────
GET  /                           → Dashboard UI
GET  /api/recommendations/<uid>  → Top-N recs for user
GET  /api/users                  → List of known users
GET  /api/metrics                → Latest model metrics
GET  /api/stats                  → Recommendation stats
"""

import os
import json
import logging
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, render_template, abort
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "user":     os.getenv("DB_USER",     "airflow"),
    "password": os.getenv("DB_PASSWORD", "airflow"),
    "dbname":   os.getenv("DB_NAME",     "recommender"),
    "port":     5432,
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def query(sql: str, params=None) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return [dict(r) for r in cur.fetchall()]


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/recommendations/<user_id>")
def get_recommendations(user_id: str):
    """GET /recommendations/user/<user_id> — Top-N most recent recommendations."""
    rows = query(
        """
        SELECT DISTINCT ON (product_id) product_id, predicted_rating
        FROM recommendations
        WHERE user_id = %s
        ORDER BY product_id, created_at DESC
        LIMIT 10
        """,
        (user_id,),
    )
    if not rows:
        return jsonify({"user_id": user_id, "recommendations": [], "message": "No recommendations yet"}), 200

    return jsonify({
        "user_id":        user_id,
        "recommendations": [r["product_id"] for r in rows],
        "details": [
            {"product_id": r["product_id"], "predicted_rating": round(float(r["predicted_rating"]), 3)}
            for r in rows
        ],
    })


@app.route("/api/users")
def list_users():
    """GET /api/users — distinct users with recommendations."""
    rows = query(
        """
        SELECT user_id, COUNT(*) as rec_count, MAX(created_at) as last_updated
        FROM recommendations
        GROUP BY user_id
        ORDER BY last_updated DESC
        LIMIT 100
        """
    )
    return jsonify({"users": rows, "total": len(rows)})


@app.route("/api/metrics")
def model_metrics():
    """GET /api/metrics — latest model training metrics."""
    # Try DB first
    rows = query("SELECT * FROM model_metrics ORDER BY created_at DESC LIMIT 1")
    if rows:
        return jsonify(rows[0])

    # Fallback: read report.json from disk
    report_path = os.getenv("REPORT_PATH", "/opt/spark-models/report.json")
    if os.path.exists(report_path):
        with open(report_path) as f:
            return jsonify(json.load(f))

    return jsonify({"message": "No metrics available yet"}), 200


@app.route("/api/stats")
def stats():
    """GET /api/stats — aggregate pipeline statistics."""
    rows = query(
        """
        SELECT
            COUNT(DISTINCT user_id)   AS total_users,
            COUNT(DISTINCT product_id) AS total_products,
            COUNT(*)                   AS total_recommendations,
            AVG(predicted_rating)      AS avg_predicted_rating,
            MAX(created_at)            AS last_recommendation_at
        FROM recommendations
        """
    )
    return jsonify(rows[0] if rows else {})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
