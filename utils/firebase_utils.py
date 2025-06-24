"""
Single-point Firebase-Admin initialisation **plus** a synchronous helper
for verifying Firebase ID tokens.  Import `verify_firebase_token()` from
your FastAPI dependencies (no await needed).
"""

from __future__ import annotations

import os
import traceback
import jwt  # PyJWT – comes in via firebase-admin

import firebase_admin
from firebase_admin import auth, credentials, exceptions as fb_exc


# ──────────────────────────────────────────────────────────────────────────────
# 🔐  Initialise the Admin SDK exactly once
# ──────────────────────────────────────────────────────────────────────────────
_CERT_PATH = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "./secrets/serviceAccountKey.json",  # ⬅︎ sensible local default
)

# Let callers override the project if they need to (e.g. multiple SA keys)
_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID")

if not firebase_admin._apps:
    cred = credentials.Certificate(_CERT_PATH)
    firebase_admin.initialize_app(
        cred,
        {"projectId": _PROJECT_ID or cred.project_id},
    )


# ──────────────────────────────────────────────────────────────────────────────
# 🔑  Public helper
# ──────────────────────────────────────────────────────────────────────────────
def verify_firebase_token(id_token: str) -> tuple[str, bool, str | None]:
    """
    Verify a Firebase ID token *synchronously*.

    Returns
    -------
    (uid, is_anonymous, email | None)

    Raises
    ------
    firebase_admin.exceptions.*  – caller should translate into HTTP 401.
    """
    if not id_token:
        raise fb_exc.InvalidIdTokenError("ID token is empty")

    # Running against the Auth emulator?  Its tokens are unsigned.
    if os.getenv("FIREBASE_AUTH_EMULATOR_HOST"):
        decoded = jwt.decode(id_token, options={"verify_signature": False})
    else:
        try:
            # Signature + expiry only – no revocation checks (quicker + fewer 401s)
            decoded = auth.verify_id_token(id_token, check_revoked=False)
        except fb_exc.FirebaseError as err:
            # Push a clear traceback to the console for debugging
            print("🛑 Firebase verification failed:", err)
            traceback.print_exc()
            raise

    uid: str = decoded["uid"]
    provider: str = decoded.get("firebase", {}).get("sign_in_provider", "")
    is_anonymous: bool = provider == "anonymous"
    email: str | None = decoded.get("email")  # may be None for anonymous users

    return uid, is_anonymous, email

