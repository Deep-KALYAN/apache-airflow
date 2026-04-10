# Initialiser Airflow
docker compose up airflow-init

# Démarrer tous les services
docker compose up -d

# Vérifier l'état
docker compose ps

# Voir les logs
docker compose logs -f