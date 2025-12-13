FROM python:3.11-slim

# On ajoute curl explicitement ici
RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir streamlink

WORKDIR /app
COPY watch.sh /app/watch.sh
RUN chmod +x /app/watch.sh

CMD ["/app/watch.sh"]