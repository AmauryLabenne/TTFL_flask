# Image Python officielle
FROM python:3.14-slim

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

# Commande de démarrage
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]
