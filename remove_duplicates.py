# remove_duplicates.py

from sqlalchemy import func
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

# ✅ Directly set the external connection string (Render EXTERNAL URL)
db_url = "postgresql+asyncpg://aoe2hd_db_user:GvoxmmKHfCMOKVKBkpx6c1mQrQZ5hHHN@dpg-cvo1fgeuk2gs73bgj3eg-a.oregon-postgres.render.com:5432/aoe2hd_db"

# ✅ Flask App + SQLAlchemy setup
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = db_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "connect_args": {"sslmode": "require"}  # Required by Render external DBs
}

db = SQLAlchemy(app)

# ✅ Minimal model definition
class GameStats(db.Model):
    __tablename__ = "game_stats"

    id = db.Column(db.Integer, primary_key=True)
    replay_hash = db.Column(db.String(64), nullable=False)

# ✅ Deduplication logic
def remove_duplicates():
    duplicates = db.session.query(
        GameStats.replay_hash,
        func.min(GameStats.id).label('keep_id'),
        func.count(GameStats.id).label('count')
    ).group_by(GameStats.replay_hash).having(func.count(GameStats.id) > 1).all()

    total_removed = 0
    for dup in duplicates:
        matches_to_delete = db.session.query(GameStats).filter(
            GameStats.replay_hash == dup.replay_hash,
            GameStats.id != dup.keep_id
        )
        deleted = matches_to_delete.delete(synchronize_session=False)
        total_removed += deleted

    db.session.commit()
    print(f"✅ Removed {total_removed} duplicates")

# ✅ Run from CLI
if __name__ == "__main__":
    with app.app_context():
        remove_duplicates()
