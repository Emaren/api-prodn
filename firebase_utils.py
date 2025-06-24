# firebase_utils.py

import os
import json
import firebase_admin
from firebase_admin import credentials, auth

def initialize_firebase():
    if firebase_admin._apps:
        return  # Already initialized

    json_key = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON")
    if json_key:
        # Load from stringified JSON
        cred = credentials.Certificate(json.loads(json_key))
    else:
        # Fallback to local file
        cred = credentials.Certificate("serviceAccountKey.json")

    firebase_admin.initialize_app(cred)

def get_user_by_uid(uid: str):
    try:
        user = auth.get_user(uid)
        return {
            "uid": user.uid,
            "email": user.email,
            "display_name": user.display_name,
        }
    except Exception as e:
        print(f"❌ Firebase get_user_by_uid error: {e}")
        return None
