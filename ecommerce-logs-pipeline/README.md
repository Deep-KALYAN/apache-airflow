# TP Jour 2 - Pipeline ETL avec Airflow & HDFS

## ��� Description

Ce projet implémente un pipeline ETL complet pour l'ingestion et l'analyse de logs e-commerce utilisant :
- **Apache Airflow 2.8** pour l'orchestration
- **Hadoop HDFS** pour le stockage distribué
- **Docker Compose** pour l'environnement de développement

## ���️ Architecture du Pipeline
Génération logs → Upload HDFS → Sensor → Analyse → Branching → Archive
(1000 lignes) (raw zone) (attente) (taux erreur) (alerte/ok) (processed zone)

text

### Tâches du DAG

| Tâche | Type | Description |
|-------|------|-------------|
| `generer_logs_journaliers` | PythonOperator | Génère 1000 lignes de logs Apache |
| `uploader_vers_hdfs` | BashOperator | Copie les logs vers HDFS (zone raw) |
| `hdfs_file_sensor` | WebHdfsSensor | Attend la disponibilité du fichier |
| `analyser_logs_hdfs` | BashOperator | Analyse status codes, top URLs, taux d'erreur |
| `brancher_selon_taux_erreur` | BranchPythonOperator | Route selon seuil 5% d'erreurs |
| `alerter_equipe_ops` | PythonOperator | Alerte si taux > 5% |
| `archiver_rapport_ok` | PythonOperator | Rapport normal si taux ≤ 5% |
| `archiver_logs_hdfs` | BashOperator | Déplace logs raw → processed |

## ��� Installation

### Prérequis

- Docker 20.10+
- Docker Compose 2.0+
- WSL2 (Windows) ou Linux/MacOS
- 8GB RAM minimum
- 10GB espace disque

### Démarrage rapide

```bash
# 1. Cloner/créer la structure
mkdir -p ecommerce-logs-pipeline/{dags,logs,plugins,scripts}
cd ecommerce-logs-pipeline

# 2. Créer les fichiers de configuration
# (copier docker-compose.yaml, Dockerfile, hadoop.env, .env)

# 3. Démarrer le stack
docker compose up -d

# 4. Créer les répertoires HDFS
docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/raw
docker exec namenode hdfs dfs -mkdir -p /data/ecommerce/logs/processed
docker exec namenode hdfs dfs -chmod -R 777 /data

# 5. Vérifier l'état
docker compose ps
��� Accès aux interfaces
Service	URL	Identifiants
Airflow UI	http://localhost:8080	admin / admin
HDFS NameNode	http://localhost:9870	-
��� Structure du projet
text
ecommerce-logs-pipeline/
├── dags/
│   ├── logs_ecommerce_dag.py    # DAG principal
│   └── test_dag.py              # DAG de test
├── logs/                         # Logs Airflow
├── plugins/                      # Plugins personnalisés
├── scripts/
│   └── generer_logs.py          # Générateur de logs Apache
├── Dockerfile                    # Image Airflow personnalisée
├── docker-compose.yaml           # Orchestration des services
├── hadoop.env                    # Configuration HDFS
├── .env                          # Variables d'environnement
└── README.md
��� Configuration
Variables d'environnement
bash
# .env
AIRFLOW_UID=50000
AIRFLOW_GID=0
HDFS Configuration (hadoop.env)
bash
HDFS_CONF_dfs_replication=1              # Facteur réplication
HDFS_CONF_dfs_webhdfs_enabled=true       # Activer WebHDFS
CLUSTER_NAME=ecommerce-cluster
Connexion Airflow → HDFS
Créer dans Airflow UI (Admin → Connections) :

Champ	Valeur
Connection Id	hdfs_default
Connection Type	WebHDFS
Host	namenode
Port	9870
Login	root
��� Tester le pipeline
1. Vérifier que le DAG est chargé
bash
docker compose exec airflow-scheduler airflow dags list | grep logs_ecommerce
2. Déclencher manuellement
Depuis l'UI Airflow :

Trouver logs_ecommerce_dag

Cliquer sur le bouton ▶️ "Trigger DAG"

3. Surveiller l'exécution
bash
# Logs du scheduler
docker compose logs airflow-scheduler -f

# État des tâches
docker compose exec airflow-scheduler airflow task state logs_ecommerce_dag <task_id> <execution_date>
4. Vérifier les fichiers dans HDFS
bash
# Liste des fichiers raw (doit être vide après archivage)
docker exec namenode hdfs dfs -ls /data/ecommerce/logs/raw/

# Liste des fichiers processed
docker exec namenode hdfs dfs -ls /data/ecommerce/logs/processed/

# Lire le contenu d'un fichier
docker exec namenode hdfs dfs -cat /data/ecommerce/logs/processed/access_YYYY-MM-DD.log | head -20
��� Commandes utiles
Gestion des conteneurs
bash
docker compose up -d              # Démarrer tous les services
docker compose down               # Arrêter tous les services
docker compose down -v            # Arrêter + supprimer les volumes
docker compose restart            # Redémarrer
docker compose logs -f            # Logs en temps réel
docker compose ps                 # État des services
Commandes HDFS
bash
# Lister les répertoires
docker exec namenode hdfs dfs -ls -R /data/

# Taille des fichiers
docker exec namenode hdfs dfs -du -h /data/

# Supprimer un fichier
docker exec namenode hdfs dfs -rm /data/ecommerce/logs/raw/access_*.log

# Rapport du cluster
docker exec namenode hdfs dfsadmin -report
Commandes Airflow
bash
# Lancer un DAG via CLI
docker compose exec airflow-scheduler airflow dags trigger logs_ecommerce_dag

# Voir les erreurs d'import
docker compose exec airflow-scheduler airflow dags list-import-errors

# Pauser/Reprendre un DAG
docker compose exec airflow-scheduler airflow dags pause logs_ecommerce_dag
docker compose exec airflow-scheduler airflow dags unpause logs_ecommerce_dag
��� Dépannage
Erreur : "No such container: airflow-scheduler"
Utilisez les noms complets :

bash
docker ps  # Trouver le nom exact
docker exec ecommerce-logs-pipeline-airflow-scheduler-1 <commande>
Erreur HdfsSensor (provider 4.0+)
Le DAG utilise WebHdfsSensor à la place de HdfsSensor (obsolète).

Les DAGs n'apparaissent pas
bash
# Redémarrer le scheduler
docker compose restart airflow-scheduler

# Vérifier les erreurs
docker compose exec airflow-scheduler airflow dags list-import-errors
Problème de permissions Docker
bash
# S'assurer que le socket Docker est monté
# Vérifier dans docker-compose.yaml:
# volumes:
#   - /var/run/docker.sock:/var/run/docker.sock
��� Tests de validation
Scénario normal (taux erreur < 5%)
Déclencher le DAG

La branche archiver_rapport_ok s'exécute

Le fichier est déplacé vers processed/

Scénario alerte (taux erreur > 5%)
Modifier generer_logs.py pour augmenter les erreurs (status 500)

La branche alerter_equipe_ops s'exécute

Un fichier d'alerte est créé dans /tmp/alert_<date>.txt

��� Ressources
Apache Airflow Documentation

Apache Hadoop HDFS

Airflow HDFS Provider

��� Auteur
DEEP KALYAN
Formation Data Engineering - IPSSI Nice

��� Version
Airflow: 2.8.0

Hadoop: 3.2.1


