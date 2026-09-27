# Bot-Twitch

Bot qui surveille la chaine d'un streamer et, des que le live demarre :

1. **se connecte en quelques secondes** (notification *push* EventSub de Twitch, pas de sondage aveugle) ;
2. **reste connecte pendant tout le live** pour compter la vue (flux `streamlink` maintenu en continu, avec relance automatique si ca coupe) ;
3. **envoie un message au tout debut du live** (par defaut : `Salut Asther ^^`).

## Comment ca marche

| Element | Role |
| --- | --- |
| `bot/eventsub.py` | WebSocket EventSub : Twitch nous pousse `stream.online` / `stream.offline` -> detection quasi instantanee. |
| `bot/main.py` | Chef d'orchestre + sondage `POLL_INTERVAL` en filet de securite si l'EventSub tombe. |
| `bot/watcher.py` | Lance `streamlink` en continu pour garder une vue ouverte, relance si le flux s'arrete. |
| `bot/chat.py` | Connexion IRC au chat pour envoyer le message d'accueil. |
| `bot/auth.py` | Token OAuth utilisateur, rafraichi automatiquement (`data/token.json`). |

Aucun compte bot necessaire : le bot utilise **ton** compte Twitch pour le chat
(c'est toi qui parles : le message d'accueil part de ton compte et tu apparais
dans la liste des chatters). Le visionnage, lui, est **anonyme** : ton token
d'application ne peut pas servir a `streamlink` (voir *Details utiles*). La vue
compte quand meme dans le compteur du streamer.

## Chrono de reaction

| Moment | Ce qui se passe |
| --- | --- |
| T+0 s | Asther lance le live. |
| Twitch (quelques secondes apres) | Twitch considere le live en ligne et envoie `stream.online` sur le WebSocket EventSub. |
| + ~0,2 s | Le bot recoit le push et lance `streamlink` -> la vue commence a compter. |
| + ~1 s | Le message d'accueil part dans le chat (la connexion IRC est deja ouverte, donc quasi instantane). |

Le sondage `POLL_INTERVAL` (20 s par defaut) n'est **pas** le mecanisme de
detection : c'est juste un filet de securite si le WebSocket casse. Tu vois la
latence reelle dans les logs : `LIVE detecte : ... | reaction en 3.4s`.

## Installation locale (Windows / Linux)

```powershell
cd Bot-Twitch
python -m venv .venv
.\.venv\Scripts\Activate.ps1        # Linux : source .venv/bin/activate
pip install -r requirements.txt
```

`streamlink` est installe par `requirements.txt` (la commande `streamlink` doit etre dispo dans le PATH).

### 1. Creer l'application Twitch

1. Va sur <https://dev.twitch.tv/console/apps> -> **Register Your Application**.
2. Nom au choix, **OAuth Redirect URLs** : `http://localhost:17563`.
3. Categorie : *Application Integration*.
4. Recupere le **Client ID** et genere un **Client Secret**.

### 2. Remplir le `.env`

```powershell
Copy-Item .env.example .env
```

Puis edite `.env` :

```ini
TWITCH_CLIENT_ID=...
TWITCH_CLIENT_SECRET=...
STREAMER_LOGIN=asther
GREETING=Salut Asther ^^
```

### 3. Autoriser le compte (une seule fois)

```powershell
python get_token.py
```

Le navigateur s'ouvre, tu autorises, le script ecrit `data/token.json` et affiche un
`TWITCH_REFRESH_TOKEN` a coller dans `.env` (utile pour Docker).

### 4. Lancer

```powershell
python -m bot
```

## Docker

```bash
cp .env.example .env   # puis remplir
docker compose up -d --build
docker compose logs -f
```

Le token est persiste dans `./data` (monte dans le conteneur), donc pas besoin de
refaire l'autorisation a chaque redemarrage. `restart: unless-stopped` remet le bot
en route apres un crash ou un reboot.

> L'autorisation OAuth (`get_token.py`) se fait **sur ta machine**, pas dans le
> conteneur : lance `python get_token.py` en local, puis copie le dossier `data/`
> a cote du `docker-compose.yml`.

## Configuration (`.env`)

| Variable | Defaut | Description |
| --- | --- | --- |
| `TWITCH_CLIENT_ID` | — | Client ID de l'application Twitch (obligatoire). |
| `TWITCH_CLIENT_SECRET` | — | Client Secret de l'application (obligatoire). |
| `STREAMER_LOGIN` | `asther` | Chaine surveillee (sans `@`). |
| `GREETING` | `Salut Asther ^^` | Message envoye au debut du live. |
| `GREETING_ENABLED` | `true` | Met `false` pour ne rien envoyer. |
| `GREETING_MAX_AGE_MINUTES` | `5` | Si le live a commence il y a plus longtemps que ca et que c'est le sondage (et non EventSub) qui l'a detecte, pas de message. `0` = toujours. |
| `WATCH_QUALITY` | `audio_only` | Qualite regardee par `streamlink` (le plus leger). |
| `POLL_INTERVAL` | `20` | Secondes entre deux verifications de secours (minimum 15). La detection normale passe par EventSub, pas par ce sondage. |
| `LOG_LEVEL` | `INFO` | `DEBUG` pour tout voir. |
| `DISCORD_WEBHOOK` | vide | Webhook optionnel pour les alertes (token mort, streamlink KO...). |
| `TWITCH_REFRESH_TOKEN` | vide | Optionnel, token de secours si `data/token.json` est absent. |

## Details utiles

- **Vitesse de connexion** : EventSub est une notification *push* : la connexion
  part dans la seconde qui suit le debut du live. Le sondage n'est qu'un secours.
- **Comptage de la vue** : `streamlink --stdout` telecharge le flux en continu
  (mute, sans lecture audio). Rien n'est enregistre sur le disque. Le flux est pris
  **en anonyme** : passer le token OAuth du compte ne marche pas, l'API interne de
  Twitch le rejette (`Unauthorized: The Authorization token is invalid`) et
  streamlink tourne alors a vide sans jamais sortir. Une vue anonyme compte
  normalement pour Twitch.
- **Si le flux se coupe** : relance automatique avec backoff, et relance forcee si
  aucun octet n'arrive pendant 2 minutes.
- **Si rien ne passe** : les dernieres lignes de `streamlink` sont affichees en
  WARNING, puis apres 3 lancements sans le moindre octet le bot alerte (Discord si
  configure) et espace les relances a 5 minutes au lieu de boucler toutes les 2
  minutes.
- **Message d'accueil** : envoye une seule fois par live (1,5 s apres la detection,
  le temps que le chat soit pret).
- **Token perime / revoque** : le bot s'arrete avec une erreur claire ; dans Docker
  il redemarre, mais il faut relancer `python get_token.py` pour le renouveler.
- **Scopes demandes** : `chat:read` et `chat:edit` uniquement (le strict minimum
  pour ecrire dans le chat).
