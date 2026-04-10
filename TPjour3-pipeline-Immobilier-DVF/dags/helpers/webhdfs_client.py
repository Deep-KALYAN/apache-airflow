"""
Client WebHDFS pour interagir avec le cluster HDFS via l'API REST.
Documentation: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
"""

import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)

WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"


class WebHDFSClient:
    """Client léger pour l'API WebHDFS d'Apache Hadoop."""

    def __init__(self, base_url: str = WEBHDFS_BASE_URL, user: str = WEBHDFS_USER):
        self.base_url = base_url.rstrip('/')
        self.user = user

    def _url(self, path: str, op: str, **params) -> str:
        """Construit l'URL WebHDFS pour une opération donnée."""
        url = f"{self.base_url}{path}?op={op}&user.name={self.user}"
        for key, value in params.items():
            url += f"&{key}={value}"
        return url

    def mkdirs(self, hdfs_path: str) -> bool:
        """
        Crée un répertoire (et ses parents) dans HDFS.
        Retourne True si succès, lève une exception sinon.
        """
        url = self._url(hdfs_path, "MKDIRS")
        logger.info(f"Création du répertoire HDFS: {url}")
        
        response = requests.put(url)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("boolean"):
                logger.info(f"Répertoire créé avec succès: {hdfs_path}")
                return True
        
        raise Exception(f"Erreur création répertoire {hdfs_path}: {response.status_code} - {response.text}")

    def upload(self, hdfs_path: str, local_file_path: str) -> str:
        """
        Uploade un fichier local vers HDFS.
        Retourne le chemin HDFS du fichier uploadé.
        Rappel: WebHDFS upload = 2 étapes:
        1. PUT sur le NameNode (allow_redirects=False) -> récupère l'URL de redirection
        2. PUT sur le DataNode avec le contenu binaire du fichier
        """
        # Étape 1: Initier l'upload sur le NameNode
        url = self._url(hdfs_path, "CREATE", overwrite="true")
        logger.info(f"Initiation upload HDFS: {url}")
        
        response = requests.put(url, allow_redirects=False)
        
        if response.status_code == 307:  # Temporary Redirect
            redirect_url = response.headers["Location"]
            logger.info(f"Redirection vers DataNode: {redirect_url}")
            
            # Étape 2: Upload vers le DataNode
            with open(local_file_path, 'rb') as f:
                upload_response = requests.put(redirect_url, data=f, headers={"Content-Type": "application/octet-stream"})
            
            if upload_response.status_code == 201:
                logger.info(f"Fichier uploadé avec succès: {hdfs_path}")
                return hdfs_path
            else:
                raise Exception(f"Erreur upload vers DataNode: {upload_response.status_code} - {upload_response.text}")
        else:
            raise Exception(f"Erreur initiation upload: {response.status_code} - {response.text}")

    def open(self, hdfs_path: str) -> bytes:
        """
        Lit le contenu d'un fichier HDFS.
        Retourne les données brutes (bytes).
        """
        url = self._url(hdfs_path, "OPEN")
        logger.info(f"Lecture fichier HDFS: {url}")
        
        response = requests.get(url, allow_redirects=False)
        
        if response.status_code == 307:
            redirect_url = response.headers["Location"]
            data_response = requests.get(redirect_url)
            if data_response.status_code == 200:
                return data_response.content
        elif response.status_code == 200:
            return response.content
        
        raise Exception(f"Erreur lecture fichier {hdfs_path}: {response.status_code}")

    def exists(self, hdfs_path: str) -> bool:
        """Vérifie si un fichier ou répertoire existe dans HDFS."""
        url = self._url(hdfs_path, "GETFILESTATUS")
        response = requests.get(url)
        return response.status_code == 200

    def list_status(self, hdfs_path: str) -> list:
        """Liste le contenu d'un répertoire HDFS."""
        url = self._url(hdfs_path, "LISTSTATUS")
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("FileStatuses", {}).get("FileStatus", [])
        else:
            raise Exception(f"Erreur listage répertoire {hdfs_path}: {response.status_code}")