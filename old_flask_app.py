import os
import logging
import traceback
import asyncio
from flask import Flask, jsonify
from flask_cors import CORS
from db import init_db_async

def create_app():
    app = Flask(__name__)

    # List the exact origins allowed for CORS
    allowed_origins = [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "https://aoe2-betting.vercel.app",
        "https://aoe2hd-frontend.onrender.com"
    ]

    # CORS settings
    CORS(
        app,
        supports_credentials=True,
        origins=allowed_origins,
        allow_headers=["Content-Type", "Authorization"],
        methods=["GET", "POST", "OPTIONS"]
    )

    # Optionally log DB URL used (just for debug, not printed by default)
    raw_db_url = os.getenv("DATABASE_URL")
    if not raw_db_url:
        user = os.getenv("PGUSER", "aoe2user")
        pw = os.getenv("PGPASSWORD", "secretpassword")
        host = os.getenv("PGHOST", "db")
        port = os.getenv("PGPORT", "5432")
        dbname = os.getenv("PGDATABASE", "aoe2db")
        raw_db_url = f"postgresql+asyncpg://{user}:{pw}@{host}:{port}/{dbname}"

    if raw_db_url.startswith("postgres://"):
        raw_db_url = raw_db_url.replace("postgres://", "postgresql+asyncpg://", 1)

    # SSL required on Render
    if "RENDER" in os.environ or "render.com" in raw_db_url:
        app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
            "connect_args": {"sslmode": "require"}
        }

    # Register API blueprints
    from routes.replay_routes import replay_bp
    from routes.user_routes import user_bp
    from routes.debug_routes import debug_bp
    from routes.admin_routes import admin_bp

    app.register_blueprint(replay_bp)
    app.register_blueprint(user_bp)
    app.register_blueprint(debug_bp)
    app.register_blueprint(admin_bp)

    # Shortcut route to current user
    @app.route("/me", methods=["GET", "POST"])
    def me_alias():
        from routes.user_routes import get_user_by_uid
        return get_user_by_uid()

    # Healthcheck route
    @app.route("/health", methods=["GET"])
    def health():
        return {"status": "ok"}, 200

    # Global error handler
    @app.errorhandler(Exception)
    def handle_exception(e):
        logging.error("Unhandled exception occurred", exc_info=e)
        return jsonify({
            "error": "Internal Server Error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }), 500

    return app

# ───────────────────────────────────────────────
# Launch server and run async DB init
# ───────────────────────────────────────────────
app = create_app()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    try:
        asyncio.run(init_db_async())  # ✅ Async tables created
        logging.info("✅ Tables created successfully")
    except Exception as e:
        logging.error(f"❌ Failed to initialize DB at startup: {e}")

    app.run(debug=True, host="0.0.0.0", port=8002)
