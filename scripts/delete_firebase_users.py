# scripts/delete_firebase_users.py

import firebase_admin
from firebase_admin import auth, credentials

cred = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(cred)

def delete_all_users():
    page = auth.list_users()
    while page:
        for user in page.users:
            print(f"Deleting user: {user.uid} ({user.email})")
            auth.delete_user(user.uid)
        page = page.get_next_page()

if __name__ == "__main__":
    delete_all_users()
    print("✅ All Firebase Auth users deleted.")
