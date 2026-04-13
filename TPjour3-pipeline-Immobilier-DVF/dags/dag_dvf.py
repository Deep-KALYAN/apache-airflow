"""
DAG: Pipeline DVF - Version corrigée
"""

from __future__ import annotations

import logging
import os
import tempfile
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import requests
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

import sys
sys.path.append('/opt/airflow')
from helpers.webhdfs_client import WebHDFSClient

logger = logging.getLogger(__name__)

DVF_URL = "https://www.data.gouv.fr/api/1/datasets/r/902db087-b0eb-4cbb-a968-0b499bde5bc4"
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF : téléchargement → HDFS raw → PostgreSQL curated",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():
    
    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        status = {}
        try:
            response = requests.head(DVF_URL, timeout=10, allow_redirects=True)
            status["dvf_api"] = response.status_code == 200
            logger.info(f"API data.gouv.fr accessible: {status['dvf_api']}")
        except Exception as e:
            status["dvf_api"] = False
            logger.error(f"Erreur accès API: {e}")
        
        try:
            client = WebHDFSClient()
            client.list_status("/")
            status["hdfs"] = True
            logger.info("HDFS accessible")
        except Exception as e:
            status["hdfs"] = False
            logger.error(f"Erreur accès HDFS: {e}")
        
        status["timestamp"] = datetime.now().isoformat()
        
        if not status["hdfs"]:
            raise Exception("Le cluster HDFS est inaccessible")
        
        return status
    
    @task(task_id="telecharger_dvf")
    def telecharger_dvf(status: dict) -> str:
        local_zip = os.path.join(tempfile.gettempdir(), "dvf_2025.zip")
        local_csv = os.path.join(tempfile.gettempdir(), "dvf_2025.csv")
        
        logger.info(f"Téléchargement depuis: {DVF_URL}")
        response = requests.get(DVF_URL, stream=True, timeout=120, allow_redirects=True)
        response.raise_for_status()
        
        total_size = 0
        with open(local_zip, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                total_size += len(chunk)
        
        logger.info(f"ZIP téléchargé: {total_size / (1024*1024):.1f} Mo")
        
        with zipfile.ZipFile(local_zip, 'r') as zip_ref:
            files = zip_ref.namelist()
            logger.info(f"Fichiers dans le ZIP: {files}")
            
            data_file = None
            for f in files:
                if f.endswith('.txt') or f.endswith('.csv'):
                    data_file = f
                    break
            
            if data_file is None:
                raise Exception("Aucun fichier .txt ou .csv trouvé dans le ZIP")
            
            with zip_ref.open(data_file) as source, open(local_csv, 'wb') as target:
                target.write(source.read())
        
        os.remove(local_zip)
        file_size = os.path.getsize(local_csv)
        logger.info(f"Fichier extrait: {local_csv} - {file_size / (1024*1024):.1f} Mo")
        
        return local_csv
    
    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        annee = datetime.now().year
        hdfs_filename = f"dvf_{annee}.csv"
        hdfs_file_path = f"{HDFS_RAW_PATH}/{hdfs_filename}"
        
        client = WebHDFSClient()
        
        if not client.exists(HDFS_RAW_PATH):
            client.mkdirs(HDFS_RAW_PATH)
        
        client.upload(hdfs_file_path, local_path)
        os.remove(local_path)
        
        logger.info(f"Fichier uploadé vers HDFS: {hdfs_file_path}")
        return hdfs_file_path
    
    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_path: str) -> dict:
        """Traite les données DVF - Version CORRIGEE"""
        client = WebHDFSClient()
        
        content = client.open(hdfs_path)
        temp_path = os.path.join(tempfile.gettempdir(), "temp_dvf.csv")
        with open(temp_path, 'wb') as f:
            f.write(content)
        
        logger.info("Lecture du fichier DVF avec séparateur '|' et décimal ','")
        
        df = pd.read_csv(
            temp_path, 
            sep='|',
            decimal=',',
            encoding='utf-8',
            low_memory=False,
            on_bad_lines='skip'
        )
        
        os.remove(temp_path)
        
        logger.info(f"Colonnes disponibles: {list(df.columns)}")
        logger.info(f"Nombre total de transactions: {len(df)}")
        
        # Renommer les colonnes
        column_mapping = {
            'Code postal': 'code_postal',
            'Valeur fonciere': 'valeur_fonciere',
            'Surface reelle bati': 'surface_reelle_bati',
            'Date mutation': 'date_mutation',
            'Type local': 'type_local',
            'Commune': 'nom_commune'
        }
        
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename(columns={old_name: new_name})
        
        # Filtrer Paris
        if 'code_postal' in df.columns:
            df['code_postal'] = df['code_postal'].astype(str)
            mask_paris = df['code_postal'].str.startswith('75', na=False)
            df = df[mask_paris].copy()
            logger.info(f"Transactions à Paris: {len(df)}")
        
        # Filtrer les appartements
        if 'type_local' in df.columns:
            mask_appart = df['type_local'].str.contains('Appartement', na=False)
            df = df[mask_appart].copy()
            logger.info(f"Appartements à Paris: {len(df)}")
        
        # Nettoyer les données numériques
        df['valeur_fonciere'] = pd.to_numeric(df['valeur_fonciere'], errors='coerce')
        df['surface_reelle_bati'] = pd.to_numeric(df['surface_reelle_bati'], errors='coerce')
        
        # Supprimer les valeurs manquantes
        df = df.dropna(subset=['valeur_fonciere', 'surface_reelle_bati'])
        
        # Filtrer les valeurs aberrantes
        df = df[df['valeur_fonciere'] > 1000]
        df = df[df['surface_reelle_bati'] > 5]
        df = df[df['surface_reelle_bati'] < 1000]
        
        # CRÉER LA COLONNE prix_m2 (LA PARTIE CRITIQUE)
        df['prix_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']
        
        # Filtrer les prix aberrants
        df = df[df['prix_m2'] > 500]
        df = df[df['prix_m2'] < 50000]
        
        logger.info(f"Données après nettoyage: {len(df)} transactions valides")
        logger.info(f"Statistiques prix/m²: min={df['prix_m2'].min():.0f}, median={df['prix_m2'].median():.0f}, max={df['prix_m2'].max():.0f}")
        
        # VÉRIFIER que la colonne existe avant groupby
        logger.info(f"Colonnes avant groupby: {list(df.columns)}")
        if 'prix_m2' not in df.columns:
            raise Exception("La colonne 'prix_m2' n'a pas été créée !")
        
        # Extraire l'arrondissement
        df['arrondissement'] = df['code_postal'].str.extract(r'75(\d{2})')
        df['arrondissement'] = pd.to_numeric(df['arrondissement'], errors='coerce')
        df = df.dropna(subset=['arrondissement'])
        df['arrondissement'] = df['arrondissement'].astype(int)
        
        # Extraire année et mois
        df['date_mutation'] = pd.to_datetime(df['date_mutation'], errors='coerce')
        df['annee'] = df['date_mutation'].dt.year
        df['mois'] = df['date_mutation'].dt.month
        df = df.dropna(subset=['annee', 'mois'])
        df['annee'] = df['annee'].astype(int)
        df['mois'] = df['mois'].astype(int)
        
        # Agrégation - VERSION SIMPLIFIÉE
        logger.info("Début de l'agrégation...")
        
        # Grouper et agréger
        grouped = df.groupby(['code_postal', 'arrondissement', 'annee', 'mois'])
        
        # Calculer les agrégats séparément pour éviter les problèmes
        result_list = []
        
        for (code_postal, arrondissement, annee, mois), group in grouped:
            result_list.append({
                'code_postal': code_postal,
                'arrondissement': int(arrondissement),
                'annee': int(annee),
                'mois': int(mois),
                'prix_m2_moyen': float(group['prix_m2'].mean()),
                'prix_m2_median': float(group['prix_m2'].median()),
                'prix_m2_min': float(group['prix_m2'].min()),
                'prix_m2_max': float(group['prix_m2'].max()),
                'nb_transactions': len(group)
            })
        
        logger.info(f"Nombre d'arrondissements traités: {len(result_list)}")
        
        # Statistiques globales
        stats_globales = {
            'annee': datetime.now().year,
            'mois': datetime.now().month,
            'nb_transactions_total': len(df),
            'prix_m2_median_paris': float(df['prix_m2'].median()),
            'prix_m2_moyen_paris': float(df['prix_m2'].mean()),
            'arrdt_plus_cher': max(result_list, key=lambda x: x['prix_m2_median'])['arrondissement'] if result_list else 0,
            'arrdt_moins_cher': min(result_list, key=lambda x: x['prix_m2_median'])['arrondissement'] if result_list else 0,
            'surface_mediane': float(df['surface_reelle_bati'].median())
        }
        
        logger.info(f"Statistiques globales: {stats_globales}")
        
        return {
            "agregats": result_list,
            "stats_globales": stats_globales
        }
    
    @task(task_id="inserer_postgresql")
    def inserer_postgresql(results: dict) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        agregats = results.get("agregats", [])
        stats_globales = results.get("stats_globales", {})
        
        nb_lignes = 0
        
        for row in agregats:
            query = """
            INSERT INTO prix_m2_arrondissement
            (code_postal, arrondissement, annee, mois, prix_m2_moyen, prix_m2_median, 
             prix_m2_min, prix_m2_max, nb_transactions, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (code_postal, annee, mois) DO UPDATE SET
                prix_m2_moyen = EXCLUDED.prix_m2_moyen,
                prix_m2_median = EXCLUDED.prix_m2_median,
                updated_at = NOW();
            """
            hook.run(query, parameters=(
                row['code_postal'],
                row['arrondissement'],
                row['annee'],
                row['mois'],
                row['prix_m2_moyen'],
                row['prix_m2_median'],
                row['prix_m2_min'],
                row['prix_m2_max'],
                row['nb_transactions']
            ))
            nb_lignes += 1
        
        # Insérer les stats globales
        stats_query = """
        INSERT INTO stats_marche
        (annee, mois, nb_transactions_total, prix_m2_median_paris,
         prix_m2_moyen_paris, arrdt_plus_cher, arrdt_moins_cher, surface_mediane, date_calcul)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (annee, mois) DO UPDATE SET
            nb_transactions_total = EXCLUDED.nb_transactions_total,
            prix_m2_median_paris = EXCLUDED.prix_m2_median_paris,
            prix_m2_moyen_paris = EXCLUDED.prix_m2_moyen_paris,
            arrdt_plus_cher = EXCLUDED.arrdt_plus_cher,
            arrdt_moins_cher = EXCLUDED.arrdt_moins_cher,
            surface_mediane = EXCLUDED.surface_mediane,
            date_calcul = NOW();
        """
        
        hook.run(stats_query, parameters=(
            stats_globales['annee'],
            stats_globales['mois'],
            stats_globales['nb_transactions_total'],
            stats_globales['prix_m2_median_paris'],
            stats_globales['prix_m2_moyen_paris'],
            stats_globales['arrdt_plus_cher'],
            stats_globales['arrdt_moins_cher'],
            stats_globales['surface_mediane']
        ))
        
        logger.info(f"Insertion terminée: {nb_lignes} lignes")
        return nb_lignes
    
    @task(task_id="generer_rapport")
    def generer_rapport(nb_inseres: int) -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        query = """
        SELECT 
            arrondissement,
            ROUND(prix_m2_median, 0) as prix_median,
            nb_transactions
        FROM prix_m2_arrondissement
        WHERE annee = %s
        ORDER BY prix_median DESC
        LIMIT 20;
        """
        
        current_year = datetime.now().year
        records = hook.get_records(query, parameters=(current_year,))
        
        rapport = "\n" + "="*60 + "\n"
        rapport += f"PRIX AU M² PAR ARRONDISSEMENT - {current_year}\n"
        rapport += "="*60 + "\n"
        
        for row in records:
            rapport += f"Arrondissement {row[0]:>2} : {row[1]:>10,} €/m² ({row[2]} transactions)\n"
        
        rapport += "="*60
        logger.info(rapport)
        
        return rapport
    
    t_verif = verifier_sources()
    t_download = telecharger_dvf(t_verif)
    t_hdfs = stocker_hdfs_raw(t_download)
    t_traiter = traiter_donnees(t_hdfs)
    t_pg = inserer_postgresql(t_traiter)
    t_rapport = generer_rapport(t_pg)
    
    chain(t_verif, t_download, t_hdfs, t_traiter, t_pg, t_rapport)


dag = pipeline_dvf()