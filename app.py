# app.py

from flask import Flask, render_template, request, redirect, send_from_directory, url_for, flash, abort
from flask_login import (
    LoginManager,
    UserMixin,
    login_user,
    login_required,
    logout_user,
    current_user,
)
from werkzeug.security import generate_password_hash, check_password_hash
from jinja2 import TemplateNotFound
import sqlite3
from pathlib import Path
import os

# Ensure Render disk path is used even if env var isn't set for the web service
os.environ.setdefault("PERSIST_DIR", "/var/data")

import threading
import time
import requests
from datetime import datetime, timezone

# JC Mechanical ingestion + DB path
from clients.jc_mechanical.ingest import run_ingestion


# ----------------------
# PATHS / PERSISTENCE
# ----------------------

BASE_DIR = Path(__file__).parent

PERSIST_DIR = Path(os.environ.get("PERSIST_DIR", BASE_DIR))
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = PERSIST_DIR / "app.db"

TEMPLATES_DIR = BASE_DIR / "templates" / "dashboards"


# ----------------------
# FLASK APP
# ----------------------

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-this-secret")

login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.init_app(app)


# ----------------------
# SQLITE HELPERS
# ----------------------

def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row  # allows access by column name
    return con


def init_db():
    with get_db() as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                dashboard_key TEXT
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS contact_submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                name TEXT NOT NULL,
                company TEXT,
                email TEXT NOT NULL,
                phone TEXT,
                systems TEXT,
                message TEXT NOT NULL,
                ip TEXT,
                user_agent TEXT
            )
            """
        )


init_db()


# ----------------------
# DASHBOARD TEMPLATE CREATION
# ----------------------

def create_dashboard_template(username: str):
    dashboard_path = TEMPLATES_DIR / f"dashboard_{username}.html"

    if dashboard_path.exists():
        return

    base_template_path = TEMPLATES_DIR / "dashboard.html"

    with open(base_template_path, "r", encoding="utf-8") as f:
        content = f.read()

    content = content.replace("{{ username }}", username)

    with open(dashboard_path, "w", encoding="utf-8") as f:
        f.write(content)


# ----------------------
# USER MODEL
# ----------------------

class User(UserMixin):
    def __init__(self, id, username, password_hash, dashboard_key):
        self.id = str(id)
        self.username = username
        self.password_hash = password_hash
        self.dashboard_key = dashboard_key

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


@login_manager.user_loader
def load_user(user_id):
    with get_db() as con:
        row = con.execute(
            """
            SELECT id, username, password_hash, dashboard_key
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()

        if row:
            return User(
                row["id"],
                row["username"],
                row["password_hash"],
                row["dashboard_key"],
            )
    return None


# ----------------------
# BACKGROUND SCHEDULER (WEB SERVICE)
# ----------------------
_scheduler_started = False


def scheduler_loop(interval_seconds: int = 3600):
    time.sleep(10)

    while True:
        try:
            print("[SCHEDULER] Starting ingestion...")
            run_ingestion(min_interval_seconds=interval_seconds)
            print("[SCHEDULER] Ingestion cycle done.")
        except Exception as e:
            print(f"[SCHEDULER] Ingestion error: {e}")

        print(f"[SCHEDULER] Sleeping {interval_seconds}s until next run...")
        time.sleep(interval_seconds)


def start_scheduler():
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    t = threading.Thread(target=scheduler_loop, args=(3600,), daemon=True)
    t.start()
    print("[SCHEDULER] Background scheduler started.")


if os.environ.get("ENABLE_INGEST_SCHEDULER", "1").strip() == "1":
    start_scheduler()


def start_ingestion_background(force: bool = False):
    """
    Fire-and-forget ingestion from a button.
    force=False -> respects min_interval_seconds (1 hour)
    force=True  -> bypasses min interval (still lock-protected)
    """
    interval = 0 if force else 3600
    t = threading.Thread(target=run_ingestion, kwargs={"min_interval_seconds": interval}, daemon=True)
    t.start()

RECAPTCHA_SITE_KEY = os.getenv("RECAPTCHA_SITE_KEY", "")
RECAPTCHA_SECRET_KEY = os.getenv("RECAPTCHA_SECRET_KEY", "")

def verify_recaptcha(token: str, remoteip: str | None = None) -> bool:
    payload = {
        "secret": RECAPTCHA_SECRET_KEY,
        "response": token,
    }
    if remoteip:
        payload["remoteip"] = remoteip

    try:
        r = requests.post(
            "https://www.google.com/recaptcha/api/siteverify",
            data=payload,
            timeout=8,
        )
        return bool(r.json().get("success", False))
    except Exception:
        return False

# ----------------------
# ROUTES
# ----------------------

@app.route("/")
def home():
    return render_template("index.html")


@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if not username or not password:
            flash("All fields are required")
            return redirect(url_for("signup"))

        password_hash = generate_password_hash(password)

        # For now: dashboard_key == username
        dashboard_key = username

        try:
            with get_db() as con:
                con.execute(
                    """
                    INSERT INTO users (username, password_hash, dashboard_key)
                    VALUES (?, ?, ?)
                    """,
                    (username, password_hash, dashboard_key),
                )

            # Create the dashboard HTML file
            create_dashboard_template(username)

            flash("Account created! Please log in.")
            return redirect(url_for("login"))

        except sqlite3.IntegrityError:
            flash("Username already exists")
            return redirect(url_for("signup"))

    return render_template("signup.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        with get_db() as con:
            row = con.execute(
                "SELECT id, username, password_hash, dashboard_key FROM users WHERE username = ?",
                (username,),
            ).fetchone()

        if row:
            user = User(row["id"], row["username"], row["password_hash"], row["dashboard_key"])
            if user.check_password(password):
                login_user(user)
                flash(f"Welcome back, {user.username}!")
                return redirect(url_for("dashboard"))

        flash("Invalid username or password")
        return redirect(url_for("login"))

    return render_template("login.html")

@app.route("/robots.txt")
def robots():
    return send_from_directory("static", "robots.txt")

@app.route("/wp-admin/<path:subpath>")
@app.route("/wordpress/<path:subpath>")
@app.route("/wp-includes/<path:subpath>")
def block_wp(subpath):
    abort(404)


@app.route("/dashboard")
@login_required
def dashboard():
    template_name = f"dashboards/dashboard_{current_user.dashboard_key}.html"

    data = {}

    if current_user.dashboard_key == "jc_mechanical":
        from clients.jc_mechanical.kpis import get_dashboard_kpis_safe
        data = get_dashboard_kpis_safe()


    all_users = []
    contact_submissions = []
    if current_user.dashboard_key == "jake":
        with get_db() as con:
            all_users = con.execute("SELECT id, username, dashboard_key FROM users").fetchall()
            contact_submissions = con.execute(
                """
                SELECT id, created_at, name, company, email, phone, systems, message
                FROM contact_submissions
                ORDER BY id DESC
                LIMIT 100
                """
            ).fetchall()

    try:
        return render_template(
            template_name,
            user=current_user,
            all_users=all_users,
            contact_submissions=contact_submissions,
            **data,
        )
    except TemplateNotFound:
        return f"No dashboard template found for {current_user.dashboard_key}.", 404

@app.route("/dashboard/leads/<int:submission_id>/delete", methods=["POST"])
@login_required
def delete_contact_submission(submission_id: int):
    # Only allow Jake/admin dashboard to delete
    if current_user.dashboard_key != "jake":
        flash("Not authorized.", "error")
        return redirect(url_for("dashboard"))

    with get_db() as con:
        # Check it exists first (optional but nice)
        row = con.execute(
            "SELECT id FROM contact_submissions WHERE id = ?",
            (submission_id,),
        ).fetchone()

        if not row:
            flash(f"Lead #{submission_id} not found.", "error")
            return redirect(url_for("dashboard"))

        con.execute(
            "DELETE FROM contact_submissions WHERE id = ?",
            (submission_id,),
        )

    flash(f"Deleted lead #{submission_id}.")
    return redirect(url_for("dashboard"))


@app.route("/dashboard/jc_mechanical/refresh", methods=["POST"])
@login_required
def refresh_jc_mechanical():
    if current_user.dashboard_key != "jc_mechanical":
        return redirect(url_for("dashboard"))

    # Start ingestion in the background (lock-protected in ingest.py)
    start_ingestion_background(force=False)

    flash("Data refresh started.")
    return redirect(url_for("dashboard"))


@app.route("/contact", methods=["GET", "POST"])
def contact():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        company = request.form.get("company", "").strip()
        email = request.form.get("email", "").strip()
        phone = request.form.get("phone", "").strip()
        systems = request.form.get("systems", "").strip()
        message = request.form.get("message", "").strip()

        # ✅ reCAPTCHA (added)
        recaptcha_token = request.form.get("g-recaptcha-response", "")
        ip = request.headers.get("X-Forwarded-For", request.remote_addr)

        if not recaptcha_token or not verify_recaptcha(recaptcha_token, ip):
            return render_template(
                "contact.html",
                success=False,
                error="Please verify that you are not a robot.",
                RECAPTCHA_SITE_KEY=RECAPTCHA_SITE_KEY,
            )

        if not name or not email or not message:
            return render_template(
                "contact.html",
                success=False,
                error="Please fill out Name, Email, and Message.",
                RECAPTCHA_SITE_KEY=RECAPTCHA_SITE_KEY,
            )

        created_at = datetime.now(timezone.utc).isoformat()
        user_agent = request.headers.get("User-Agent", "")

        with get_db() as con:
            con.execute(
                """
                INSERT INTO contact_submissions
                (created_at, name, company, email, phone, systems, message, ip, user_agent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (created_at, name, company, email, phone, systems, message, ip, user_agent),
            )

        return redirect(url_for("contact", sent="1"))

    success = request.args.get("sent") == "1"
    return render_template("contact.html", success=success, RECAPTCHA_SITE_KEY=RECAPTCHA_SITE_KEY)


@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("You have been logged out.")
    return redirect(url_for("home"))


if __name__ == "__main__":
    # Render provides PORT
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
