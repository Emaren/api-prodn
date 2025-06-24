# dependencies/auth.py

from fastapi import Request, HTTPException, status
from firebase_admin import auth, credentials, initialize_app
import firebase_admin

# ✅ Ensure Firebase is initialized once
if not firebase_admin._apps:
    cred = credentials.Certificate("secrets/serviceAccountKey.json")  # Ensure this path is correct
    initialize_app(cred)

async def get_firebase_user(request: Request):
    print("🔎 Incoming Headers:", dict(request.headers))  # Debug incoming headers

    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Authorization header missing or malformed",
        )

    id_token = auth_header.removeprefix("Bearer ").strip()
    print(f"🧪 Received Firebase token: {id_token[:40]}...")

    try:
        decoded_token = auth.verify_id_token(id_token)
        uid = decoded_token.get("uid")
        email = decoded_token.get("email", "")
        is_anon = decoded_token.get("firebase", {}).get("sign_in_provider") == "anonymous"

        print(f"✅ Firebase verified UID: {uid} | Email: {email}")
        return {"uid": uid, "email": email, "is_anonymous": is_anon}
    except Exception as e:
        import traceback
        print("❌ Token verification failed:")
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired Firebase token",
        )

