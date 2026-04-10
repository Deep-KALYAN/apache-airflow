import requests
import logging
from airflow.decorators import dag, task
from datetime import datetime, timedelta

# 1. Define the regions as required by the TP
REGIONS = {
    "Ile-de-France": {"lat": 48.8566, "lon": 2.3522},
    "Occitanie": {"lat": 43.6047, "lon": 1.4442},
    "PACA": {"lat": 43.2965, "lon": 5.3698},
    "Grand Est": {"lat": 48.5734, "lon": 7.7521},
    "Bretagne": {"lat": 48.1173, "lon": -1.6778}
}

# 2. Use the @dag decorator to tell Airflow this is a pipeline
@dag(
    dag_id="energie_meteo_dag",
    schedule_interval="0 6 * * *", # Run at 6:00 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['RTE', 'TP_IPSSI']
)
def energie_meteo_pipeline():

    @task()
    def verifier_apis():
        apis = {
            "Open-Meteo": "https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&daily=sunshine_duration&timezone=Europe/Paris&forecast_days=1",
            "éCO2mix": "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/records?limit=1"
        }
        for name, url in apis.items():
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                logging.info(f"{name} is available.")
            else:
                raise ValueError(f"API {name} is down (Status: {response.status_code})")

    @task()
    def collecter_meteo_regions():
        results = {}
        for region, coords in REGIONS.items():
            params = {
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "daily": "sunshine_duration,wind_speed_10m_max",
                "timezone": "Europe/Paris",
                "forecast_days": 1,
            }
            resp = requests.get("https://api.open-meteo.com/v1/forecast", params=params)
            data = resp.json()
            
            sun_h = data["daily"]["sunshine_duration"][0] / 3600 
            wind_k = data["daily"]["wind_speed_10m_max"][0]
            
            results[region] = {"ensoleillement_h": round(sun_h, 2), "vent_kmh": wind_k}
        return results

    # # Define the sequence of execution
    # check = verifier_apis()
    # weather_data = collecter_meteo_regions()
    
    # check >> weather_data

    @task()
    def collecter_production_electrique():
        region_map = {
            "Ile-de-France": "Île-de-France",
            "Occitanie": "Occitanie",
            "PACA": "Provence-Alpes-Côte d'Azur",
            "Grand Est": "Grand Est",
            "Bretagne": "Bretagne"
        }
        
        prod_results = {}
        base_url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/records"
        
        for key, api_name in region_map.items():
            # Standard quote, no doubling needed here
            # Requests will handle the 'ô' and the "'" automatically
            params = {
                "where": f"libelle_region=\"{api_name}\"", # Use double quotes for the value
                "order_by": "date_heure DESC",
                "limit": 24 
            }
            
            try:
                logging.info(f"Fetching production for: {api_name}")
                resp = requests.get(base_url, params=params, timeout=15)
                resp.raise_for_status()
                records = resp.json().get("results", [])
                
                if not records:
                    logging.warning(f"No records found for {api_name}")
                    prod_results[key] = {"solaire_mw": 0, "eolien_mw": 0}
                    continue

                total_solar = 0
                total_wind = 0
                for r in records:
                    # Some values might be None in the API
                    total_solar += float(r.get("solaire") or 0)
                    total_wind += float(r.get("eolien") or 0)
                
                count = len(records)
                prod_results[key] = {
                    "solaire_mw": round(total_solar / count, 2), 
                    "eolien_mw": round(total_wind / count, 2)
                }
                
            except Exception as e:
                logging.error(f"Critical error on {key}: {e}")
                raise
            
        return prod_results
    
    @task()
    def analyser_correlation(meteo_data: dict, prod_data: dict):
        alerts = []
        
        for region in meteo_data.keys():
            sun = meteo_data[region]["ensoleillement_h"]
            wind = meteo_data[region]["vent_kmh"]
            prod_solar = prod_data[region]["solaire_mw"]
            prod_wind = prod_data[region]["eolien_mw"]
            
            # Rule 1: High sun, low solar production
            if sun > 6 and prod_solar <= 1000:
                alerts.append(f"ALERT SOLAR: {region} (Sun: {sun}h, Prod: {prod_solar}MW)")
            
            # Rule 2: High wind, low wind production
            if wind > 30 and prod_wind <= 2000:
                alerts.append(f"ALERT WIND: {region} (Wind: {wind}km/h, Prod: {prod_wind}MW)")
        
        if not alerts:
            logging.info("No anomalies detected.")
        else:
            for a in alerts:
                logging.warning(a)
                
        return {"total_alerts": len(alerts), "details": alerts}
    
    @task()
    def generer_rapport_energie(analysis_results: dict):
        import json
        from datetime import datetime
        
        # Create a filename with the current date
        date_str = datetime.now().strftime("%Y-%m-%d")
        file_path = f"/tmp/rapport_energie_{date_str}.json"
        
        report_data = {
            "date_execution": date_str,
            "statistiques": analysis_results,
            "statut_global": "ANOMALIE" if analysis_results["total_alerts"] > 0 else "OK"
        }
        
        with open(file_path, "w") as f:
            json.dump(report_data, f, indent=4)
            
        logging.info(f"Rapport généré avec succès dans : {file_path}")
        return file_path
    # Define the execution flow
    check = verifier_apis()
    m_data = collecter_meteo_regions()
    p_data = collecter_production_electrique()
    
    # Analyze needs both outputs
    analysis = analyser_correlation(m_data, p_data)
    
    # New final task
    report = generer_rapport_energie(analysis)
    
    # Task dependencies
    check >> [m_data, p_data] >> analysis >> report

# 3. Instantiate the DAG
dag_instance = energie_meteo_pipeline()