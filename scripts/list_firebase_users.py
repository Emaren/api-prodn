from firebase_admin import auth, credentials, initialize_app
from pprint import pprint

cred = credentials.Certificate("serviceAccountKey.json")
initialize_app(cred)

users = auth.list_users().users
for user in users:
    pprint({
        "uid": user.uid,
        "email": user.email,
        "display_name": user.display_name,
        "email_verified": user.email_verified,
        "disabled": user.disabled,
        "custom_claims": user.custom_claims,
        "provider_data": [vars(p) for p in user.provider_data],
    })
