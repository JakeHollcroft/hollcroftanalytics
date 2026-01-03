from flask import Flask, render_template, request, redirect, url_for, flash, Response
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from jinja2 import TemplateNotFound
import sqlite3
from pathlib import Path
import subprocess
import duckdb
import os
import threading
import time
from datetime import datetime, timezone


from clients.jc_mechanical.ingest import run_ingestion
from clients.jc_mechanical.config import DB_FILE

def start_ingestion_background():
    thread = threading.Thread(target=run_ingestion, daemon=True)
    thread.start()


def ensure_data():
    need_ingest = False
    if not DB_FILE.exists():
        print("DB file not found. Running ingestion...")
        need_ingest = True
    else:
        # check if 'jobs' table exists
        conn = duckdb.connect(DB_FILE)
        try:
            conn.execute("SELECT 1 FROM jobs LIMIT 1").fetchall()
        except duckdb.CatalogException:
            print("Jobs table missing. Running ingestion...")
            need_ingest = True
        finally:
            conn.close()

    if need_ingest:
        run_ingestion()

ensure_data()

BASE_DIR = Path(__file__).parent

PERSIST_DIR = Path(os.environ.get("PERSIST_DIR", BASE_DIR))
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = PERSIST_DIR / "app.db"


app = Flask(__name__)
app.secret_key = "change-this-secret"

login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.init_app(app)

# ----------------------
# DATABASE
# ----------------------

def get_db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row  # allows access by column name
    return con

def init_db():
    with get_db() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            dashboard_key TEXT
        )
        """)

        con.execute("""
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
        """)


init_db()

TEMPLATES_DIR = BASE_DIR / "templates" / "dashboards"



_scheduler_started = False

def scheduler_loop(interval_seconds=3600):
    # Optional: delay so app boots fully before first run
    time.sleep(10)

    while True:
        start = time.time()
        try:
            print("[SCHEDULER] Starting ingestion...")
            run_ingestion()
            print("[SCHEDULER] Ingestion finished.")
        except Exception as e:
            print(f"[SCHEDULER] Ingestion error: {e}")

        elapsed = time.time() - start
        sleep_for = max(0, interval_seconds - elapsed)
        print(f"[SCHEDULER] Next run in {sleep_for:.0f}s")
        time.sleep(sleep_for)

def start_scheduler():
    global _scheduler_started
    if _scheduler_started:
        return

    _scheduler_started = True
    t = threading.Thread(target=scheduler_loop, args=(3600,), daemon=True)
    t.start()
    print("[SCHEDULER] Background scheduler started.")


def create_dashboard_template(username):
    dashboard_path = TEMPLATES_DIR / f"dashboard_{username}.html"

    # Do nothing if it already exists
    if dashboard_path.exists():
        return

    base_template_path = TEMPLATES_DIR / "dashboard.html"

    with open(base_template_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Optional: personalize the file itself
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
            (user_id,)
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
# ROUTES
# ----------------------


@app.route("/dashboard/jc_mechanical/refresh", methods=["POST"])
@login_required
def refresh_jc_mechanical():
    if current_user.dashboard_key != "jc_mechanical":
        flash("Unauthorized.")
        return redirect(url_for("dashboard"))

    from clients.jc_mechanical.kpis import get_refresh_status

    status = get_refresh_status()

    if not status["can_refresh"]:
        flash(f"You can refresh again at {status['next_refresh']}")
        return redirect(url_for("dashboard"))

    start_ingestion_background()
    flash("Data refresh started.")
    return redirect(url_for("dashboard"))


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
                (username,)
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



@app.route("/dashboard")
@login_required
def dashboard():
    template_name = f"dashboards/dashboard_{current_user.dashboard_key}.html"

    # Dynamically import KPI functions only for dashboards that need them
    data = {}

    if current_user.dashboard_key == "jc_mechanical":
        from clients.jc_mechanical.kpis import get_dashboard_kpis
        data = get_dashboard_kpis()
    
    # For "jake" dashboard, fetch all users
    all_users = []
    contact_submissions = []
    if current_user.dashboard_key == "jake":
        with get_db() as con:
            all_users = con.execute("SELECT id, username, dashboard_key FROM users").fetchall()
            contact_submissions = con.execute("""
                SELECT id, created_at, name, company, email, phone, systems, message
                FROM contact_submissions
                ORDER BY id DESC
                LIMIT 100
            """).fetchall()

    try:
        return render_template(
            template_name,
            user=current_user,
            all_users=all_users,
            contact_submissions=contact_submissions,
            **data
        )
    except TemplateNotFound:
        return f"No dashboard template found for {current_user.dashboard_key}.", 404

@app.route("/contact", methods=["GET", "POST"])
def contact():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        company = request.form.get("company", "").strip()
        email = request.form.get("email", "").strip()
        phone = request.form.get("phone", "").strip()
        systems = request.form.get("systems", "").strip()
        message = request.form.get("message", "").strip()

        if not name or not email or not message:
            return render_template(
                "contact.html",
                success=False,
                error="Please fill out Name, Email, and Message."
            )

        created_at = datetime.now(timezone.utc).isoformat()
        ip = request.headers.get("X-Forwarded-For", request.remote_addr)
        user_agent = request.headers.get("User-Agent", "")

        with get_db() as con:
            con.execute("""
                INSERT INTO contact_submissions
                (created_at, name, company, email, phone, systems, message, ip, user_agent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (created_at, name, company, email, phone, systems, message, ip, user_agent))

        return redirect(url_for("contact", sent="1"))

    success = (request.args.get("sent") == "1")
    return render_template("contact.html", success=success)


@app.route("/logout")
@login_required
def logout():
    logout_user()
    flash("You have been logged out.")
    return redirect(url_for("home"))

# if __name__ == "__main__":
#     app.run(debug=True)

start_scheduler()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
