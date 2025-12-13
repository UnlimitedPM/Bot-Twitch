#!/bin/bash

# Fonction d'alerte
send_discord_alert() {
    echo "⚠️ ENVOI ALERTE DISCORD..."
    # On utilise curl pour poster le message
    curl -H "Content-Type: application/json" \
         -X POST \
         -d '{"content": "🚨 **ALERTE BOT** 🚨\nLe Token Twitch est invalide (Erreur 401). Ton bot est en pause !"}' \
         "$DISCORD_WEBHOOK"
}

echo "Cible : $STREAMER_URL"

while true; do
    echo "[$(date '+%H:%M:%S')] Recherche du live..."

    # On capture la sortie d'erreur de streamlink dans une variable "OUTPUT"
    # 2>&1 permet de mélanger les erreurs avec la sortie standard pour tout capturer
    OUTPUT=$(streamlink --twitch-api-header "Authorization=OAuth $TWITCH_TOKEN" \
                        "$STREAMER_URL" \
                        audio_only \
                        --stdout \
                        2>&1 > /dev/null)

    EXIT_CODE=$?

    # --- Analyse ---
    
    # Si le token est invalide, Streamlink écrit "401 Client Error" ou "Unauthorized"
    # On cherche ces mots dans la variable OUTPUT
    if echo "$OUTPUT" | grep -q "401 Client Error" || echo "$OUTPUT" | grep -q "Unauthorized"; then
        echo "⛔ ERREUR TOKEN DETECTÉE !"
        echo "Détail de l'erreur : $OUTPUT"
        send_discord_alert
        sleep 43200 
        continue
    fi

    # Si tout va bien (Stream fini ou Pas de live)
    if [ $EXIT_CODE -eq 0 ]; then
         echo "[$(date '+%H:%M:%S')] Live terminé."
    else
         # On affiche l'erreur seulement si ce n'est pas juste "Stream offline"
         # Pour éviter de spammer tes logs
         if ! echo "$OUTPUT" | grep -q "No playable streams found"; then
             echo "[$(date '+%H:%M:%S')] Erreur technique : $OUTPUT"
         else
             echo "[$(date '+%H:%M:%S')] Pas de live."
         fi
    fi

    echo "Attente 60s..."
    sleep 60
done