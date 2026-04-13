# Pipeline Immobilier DVF - Airflow + HDFS + PostgreSQL

## 📋 Description

Ce projet implémente un pipeline ETL complet orchestré par **Apache Airflow** pour traiter les données des **Demandes de Valeurs Foncières (DVF)**. Le pipeline télécharge les données immobilières depuis data.gouv.fr, les stocke dans un **Data Lake (HDFS)** et les agrège dans un **Data Warehouse (PostgreSQL)** pour l'analyse des prix de l'immobilier à Paris.

## 🏗️ Architecture
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ data.gouv.fr │────▶│ Airflow DAG │────▶│ HDFS (Raw) │
│ (Source DVF) │ │ (Orchestration)│ │ Data Lake │
└─────────────────┘ └────────┬────────┘ └─────────────────┘
│
▼
┌─────────────────┐
│ PostgreSQL │
│ (Curated Zone) │
└─────────────────┘


### Composants

| Service | Rôle | Port |
|---------|------|------|
| **Airflow Webserver** | Interface UI d'orchestration | 8080 |
| **Airflow Scheduler** | Ordonnanceur des tâches | - |
| **PostgreSQL** | Base de données (metadata + curated) | 5432 |
| **HDFS NameNode** | Gestionnaire des métadonnées HDFS | 9870 |
| **HDFS DataNode** | Stockage physique des données | 9864 |

## 📊 Structure du Pipeline

Le DAG `pipeline_dvf_immobilier` contient 6 tâches :

```python
verifier_sources → telecharger_dvf → stocker_hdfs_raw → traiter_donnees → inserer_postgresql → generer_rapport


Tâche	Description
verifier_sources	Vérifie l'accessibilité de l'API data.gouv.fr et HDFS
telecharger_dvf	Télécharge et extrait le fichier ZIP depuis data.gouv.fr
stocker_hdfs_raw	Uploade le fichier CSV vers HDFS (zone raw)
traiter_donnees	Filtre Paris, calcule prix/m², agrège par arrondissement
inserer_postgresql	Insère les agrégats dans PostgreSQL (UPSERT)
generer_rapport	Génère un classement des arrondissements par prix


🚀 Installation et Démarrage
Prérequis
Docker Desktop 4.x+

Docker Compose v2

8 Go RAM minimum

WSL2 (pour Windows)

1. Cloner le projet
bash
git clone <votre-repo>
cd TPjour3-pipeline-Immobilier-DVF
2. Créer la structure des répertoires
bash
mkdir -p dags logs plugins sql dags/helpers
3. Démarrer l'infrastructure
bash
# Initialiser Airflow (création base + utilisateur admin)
docker compose up airflow-init

# Démarrer tous les services
docker compose up -d

# Vérifier que tous les services sont healthy
docker compose ps
4. Accéder aux interfaces
Interface	URL	Identifiants
Airflow UI	http://localhost:8080	admin / admin
HDFS NameNode	http://localhost:9870	-
PostgreSQL	localhost:5432	airflow / airflow
🔧 Utilisation
Déclencher le DAG
Depuis l'interface Airflow :

Activez le DAG pipeline_dvf_immobilier

Cliquez sur le bouton "Play" (▶️)

Cliquez sur "Trigger DAG"

Depuis la ligne de commande :

bash
docker exec -it dvf-airflow-scheduler airflow dags trigger pipeline_dvf_immobilier
Surveiller l'exécution
bash
# Voir les logs du scheduler
docker compose logs -f airflow-scheduler

# Voir les logs d'une tâche spécifique
docker exec -it dvf-airflow-scheduler airflow tasks test pipeline_dvf_immobilier <task_id> 2025-01-01
📈 Résultats
Structure des tables PostgreSQL
prix_m2_arrondissement - Prix au m² par arrondissement

Colonne	Type	Description
code_postal	VARCHAR(10)	Code postal
arrondissement	INTEGER	Arrondissement (1-20)
annee	INTEGER	Année de mutation
mois	INTEGER	Mois de mutation
prix_m2_median	NUMERIC(10,2)	Prix médian au m²
nb_transactions	INTEGER	Nombre de transactions
stats_marche - Statistiques globales Paris

Colonne	Type	Description
annee	INTEGER	Année
nb_transactions_total	INTEGER	Total transactions
prix_m2_median_paris	NUMERIC(10,2)	Prix médian Paris
Exemple de requête
sql
-- Top 10 des arrondissements les plus chers
SELECT arrondissement, 
       ROUND(prix_m2_median, 0) as prix_median,
       nb_transactions
FROM prix_m2_arrondissement 
WHERE annee = 2025 
ORDER BY prix_m2_median DESC 
LIMIT 10;
🛠️ Dépannage
Problèmes courants
Problème	Solution
Permission denied (logs)	Ignorer, n'affecte pas l'exécution
HDFS refuse la connexion	Attendre 30s que le NameNode démarre
Table missing columns	Exécuter les ALTER TABLE dans le README
Base dvf n'existe pas	docker exec -it dvf-postgres psql -U airflow -c "CREATE DATABASE dvf;"
Commandes utiles
bash
# Redémarrer tous les services
docker compose restart

# Voir les logs d'un service spécifique
docker compose logs -f <service_name>

# Nettoyer toutes les données (reset complet)
docker compose down -v

# Se connecter à PostgreSQL
docker exec -it dvf-postgres psql -U airflow -d dvf

# Vérifier HDFS
curl "http://localhost:9870/webhdfs/v1/?op=LISTSTATUS&user.name=root"
📁 Structure du projet
text
TPjour3-pipeline-Immobilier-DVF/
├── docker-compose.yaml          # Infrastructure complète
├── dags/
│   ├── dag_dvf.py               # DAG Airflow principal
│   └── helpers/
│       └── webhdfs_client.py    # Client WebHDFS
├── sql/
│   └── init_dvf.sql             # Initialisation PostgreSQL
├── logs/                        # Logs Airflow (ignoré)
├── plugins/                     # Plugins Airflow (ignoré)
└── README.md                    # Ce fichier
🔄 Format des données DVF 2025
Le fichier DVF 2025 utilise :

Séparateur : | (pipe)

Décimales : , (virgule)

Encodage : UTF-8

Extension : .txt dans une archive .zip

📊 Statistiques du traitement
Transactions totales : ~3.7 millions

Transactions Paris : ~86 000

Appartements Paris : ~38 000

Transactions valides : ~32 000

Arrondissements traités : 20

🏆 Points clés réalisés
✅ Infrastructure Docker Compose complète

✅ Client WebHDFS personnalisé

✅ DAG Airflow avec TaskFlow API

✅ Lecture du nouveau format DVF (séparateur |)

✅ Calcul des prix au m² et agrégation

✅ UPSERT PostgreSQL (idempotent)

✅ Stockage raw dans HDFS

✅ Interface WebHDFS fonctionnelle

📚 Ressources
Documentation Airflow

WebHDFS API

Données DVF

Image Docker Hadoop

👥 Auteurs
Formation Apache Airflow - IPSSI Montpellier

Jour 3 - Intégration Docker et Kubernetes

📄 Licence
Ce projet est fourni à des fins éducatives.

text

## Sauvegarder le fichier

```bash
# Créer le fichier README.md
cat > README.md << 'EOF'
[Copier le contenu ci-dessus]
EOF

# Vérifier que le fichier a été créé
cat README.md | head -20