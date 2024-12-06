FROM python:3.8-slim-buster

# Installer les dépendances Python directement
RUN pip install mysql-connector-python dash sqlalchemy pandas

# Copier votre application Dash dans le conteneur
COPY ./dash /app

# Définir le répertoire de travail
WORKDIR /app

# Exposer le port 5000 pour l'application Dash
EXPOSE 5000

# Lancer l'application Dash
CMD ["python", "my_dash.py"]