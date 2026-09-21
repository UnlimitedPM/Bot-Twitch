"""Obtention du token Twitch (a lancer une seule fois, en local).

    python get_token.py

Ouvre ton navigateur, tu autorises l'application, et le script ecrit
`data/token.json` ainsi que le `TWITCH_REFRESH_TOKEN` a mettre dans `.env`.
"""

from __future__ import annotations

import json
import os
import secrets
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dotenv import load_dotenv

from bot.auth import Token, TokenStore
from bot.config import ROOT, TOKEN_FILE

AUTHORIZE_URL = "https://id.twitch.tv/oauth2/authorize"
TOKEN_URL = "https://id.twitch.tv/oauth2/token"
VALIDATE_URL = "https://id.twitch.tv/oauth2/validate"

SCOPES = ["chat:read", "chat:edit"]
REDIRECT_PORT = 17563
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}"
TIMEOUT = 300

_result: dict[str, str] = {}
_done = threading.Event()


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 (nom impose par http.server)
        query = urllib.parse.urlparse(self.path).query
        params = urllib.parse.parse_qs(query)
        _result.update({k: v[0] for k, v in params.items()})
        _done.set()

        ok = "code" in _result
        body = (
            "<h2>Autorisation OK, tu peux fermer cet onglet.</h2>"
            if ok
            else "<h2>Echec de l'autorisation.</h2>"
        )
        payload = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args) -> None:  # silence les logs du serveur local
        return


def _post(url: str, data: dict[str, str]) -> dict:
    body = urllib.parse.urlencode(data).encode("utf-8")
    request = urllib.request.Request(url, data=body)
    with urllib.request.urlopen(request, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get(url: str, token: str) -> dict:
    request = urllib.request.Request(url, headers={"Authorization": f"OAuth {token}"})
    with urllib.request.urlopen(request, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> int:
    load_dotenv(ROOT / ".env")

    client_id = (os.getenv("TWITCH_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("TWITCH_CLIENT_SECRET") or "").strip()
    if not client_id or not client_secret:
        print(
            "Il manque TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET.\n"
            f"Copie .env.example vers .env ({ROOT / '.env'}) et remplis-les, "
            "puis relance ce script."
        )
        return 1

    state = secrets.token_urlsafe(16)
    params = {
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "state": state,
        "force_verify": "true",
    }
    url = f"{AUTHORIZE_URL}?{urllib.parse.urlencode(params)}"

    server = ThreadingHTTPServer(("127.0.0.1", REDIRECT_PORT), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    print("Ouverture du navigateur pour autoriser l'application...")
    print(f"Si rien ne s'ouvre, va manuellement sur :\n{url}\n")
    webbrowser.open(url)

    if not _done.wait(TIMEOUT):
        print("Delai depasse, aucune autorisation recue.")
        return 1
    server.shutdown()

    if _result.get("error"):
        print(f"Twitch a refuse : {_result['error_description'] or _result['error']}")
        return 1
    if _result.get("state") != state:
        print("Etat OAuth invalide, on arrete par securite.")
        return 1

    code = _result.get("code")
    if not code:
        print("Pas de code d'autorisation recu.")
        return 1

    try:
        payload = _post(
            TOKEN_URL,
            {
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": REDIRECT_URI,
            },
        )
    except urllib.error.HTTPError as exc:
        print(f"Echange du code refuse ({exc.code}) : {exc.read().decode('utf-8', 'replace')}")
        return 1

    access_token = payload["access_token"]
    refresh_token = payload["refresh_token"]
    expires_in = int(payload.get("expires_in", 0))

    try:
        info = _get(VALIDATE_URL, access_token)
    except urllib.error.HTTPError as exc:
        print(f"Token refuse ({exc.code}) : {exc.read().decode('utf-8', 'replace')}")
        return 1

    scopes = list(info.get("scopes", []))
    missing = [scope for scope in SCOPES if scope not in scopes]
    if missing:
        print("Scopes manquants : " + ", ".join(missing) + "\nRefais l'autorisation.")

    token = Token(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=time.time() + expires_in,
        user_id=str(info.get("user_id", "")),
        login=info.get("login", ""),
        scopes=scopes,
    )
    TokenStore(TOKEN_FILE).save(token)

    print("\n" + "=" * 60)
    print(f"Token enregistre pour : {token.login} (id {token.user_id})")
    print(f"Fichier : {TOKEN_FILE}")
    print("Scopes  : " + ", ".join(scopes))
    print("=" * 60)
    print("\nAjoute cette ligne dans ton .env (utile pour Docker) :\n")
    print(f"TWITCH_REFRESH_TOKEN={refresh_token}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
