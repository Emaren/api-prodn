# scripts/sync_firebase_users.py

import firebase_admin
from firebase_admin import credentials, auth
import psycopg2
from psycopg2.extras import execute_values
import os

# Load Firebase Admin SDK
cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

# Postgres connection config
PG_HOST = "127.0.0.1"
PG_DB = "aoe2db"
PG_USER = "aoe2user"
PG_PASSWORD = "secretpassword"

# Fetch all Firebase users
def get_firebase_users():
    all_users = []
    page = auth.list_users()
    while page:
        for user in page.users:
            all_users.append(user)
        page = page.get_next_page()
    return all_users

# Sync users to Postgres
def sync_users_to_postgres(users):
    conn = psycopg2.connect(
        host=PG_HOST,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    cur = conn.cursor()

    for user in users:
        uid = user.uid
        email = user.email or None
        # Extract username from email
        if email and "@aoe2hdbets.com" in email:
            raw_name = email.split("@")[0]
            if raw_name.count("-") >= 4:  # UUID-style garbage
                in_game_name = f"firebase_{uid[:6]}"
            else:
                in_game_name = raw_name
        else:
            in_game_name = "unknown"

        # Special override for our real users (safe because it's only 3)
        if email == "81ab8226-23b1-4952-b345-30e79a1cbcf8@aoe2hdbets.com":
            in_game_name = "Emaren"
            is_admin = True
        elif email == "5d3fb847-1edd-4fc4-8e91-6ebc4975d893@aoe2hdbets.com":
            in_game_name = "Tralalero Tralala"
            is_admin = False
        elif email == "70f7fdef-02c8-4c39-8dd9-30dff01eafde@aoe2hdbets.com":
            in_game_name = "AS_godofredo"
            is_admin = False
        else:
            is_admin = False

        # Insert or update safely
        cur.execute("""
            INSERT INTO users (uid, email, in_game_name, verified, wallet_address, lock_name, is_admin, created_at)
            VALUES (%s, %s, %s, false, '', false, %s, NOW())
            ON CONFLICT (uid) DO NOTHING;
        """, (uid, email, in_game_name, is_admin))

        print(f"✅ Synced: {in_game_name} ({email})")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    users = get_firebase_users()
    sync_users_to_postgres(users)
    print("🎯 All Firebase users synced to Postgres.")
