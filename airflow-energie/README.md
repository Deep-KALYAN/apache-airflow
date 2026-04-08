# ⚡ Pipeline Énergie & Météo - RTE Data Engineering
Ce projet est une solution d'orchestration de données développée avec Apache Airflow. L'objectif est de surveiller la cohérence entre les prévisions météorologiques et la production réelle d'énergies renouvelables (solaire et éolien) dans 5 régions clés de France.

# 📋 Fonctionnalités du Pipeline
Le DAG energie_meteo_dag s'exécute quotidiennement et réalise les étapes suivantes :

Vérification API : S'assure de la disponibilité des services Open-Meteo et éCO2mix (RTE).

Collecte Météo : Récupère la durée d'ensoleillement et la vitesse du vent via Open-Meteo.

Collecte Production : Extrait la production moyenne (MW) sur les dernières 24h depuis l'API éCO2mix.

Analyse de Corrélation : Détecte les anomalies basées sur des règles métier :

Alerte Solaire : Ensoleillement > 6h et Production ≤ 1000 MW.

Alerte Éolienne : Vent > 30 km/h et Production ≤ 2000 MW.

Reporting : Génère un rapport final au format JSON.

# 🛠️ Architecture Technique
Orchestrateur : Apache Airflow 2.8.0.

Conteneurisation : Docker / Docker Compose (CeleryExecutor avec Redis et PostgreSQL).

Langage : Python 3.8+ (Utilisation des @task et @dag decorators).

Gestion des données : XComs pour le passage de dictionnaires entre tâches.

# 🚀 Installation et Lancement
Cloner le projet et se placer dans le répertoire :

Bash
cd airflow-energie

Bash
docker compose up airflow-init
docker compose up -d
Accéder à l'interface :
Rendez-vous sur http://localhost:8080 (Identifiants : airflow / airflow).

# 🔍 Robustesse et "Edge Cases"
Le pipeline a été conçu pour être résilient face aux problèmes courants de la donnée réelle :

Encodage URL : Gestion spécifique des caractères spéciaux et accents pour les régions (ex: Provence-Alpes-Côte d'Azur).

Nettoyage de données : Conversion explicite des types (String vers Float) pour éviter les erreurs de calcul lors des sommes.

Idempotence : Le pipeline peut être relancé pour une même date sans corrompre les résultats précédents.

# 📂 Structure du Projet
dags/energie_meteo_dag.py : Code source du pipeline.

docker-compose.yaml : Configuration de l'infrastructure Airflow.

REPONSES.md : Réponses théoriques aux questions du TP.

output/ : Contient les rapports JSON générés (si le volume est configuré).