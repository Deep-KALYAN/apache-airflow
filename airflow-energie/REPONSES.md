Q1 — Docker Executor

LocalExecutor : Exécute les tâches en tant que processus locaux sur le même nœud que le scheduler. Limité par les ressources d'une seule machine.
Contexte RTE : Idéal pour le développement ou des petits pipelines internes.

CeleryExecutor : Distribue les tâches sur une flotte de "workers" distincts via une file d'attente (Redis/RabbitMQ).
Contexte RTE : Production standard pour gérer des centaines de flux régionaux en parallèle. Haute scalabilité horizontale.

KubernetesExecutor : Crée un pod éphémère pour chaque tâche.
Contexte RTE : Idéal pour une infrastructure Cloud/Auto-scalée. Optimise les ressources car elles ne sont consommées que durant l'exécution de la tâche.

Q2 — Volumes Docker et persistance des DAGs

Mécanisme : Le bind mount lie directement un dossier de l'hôte au conteneur. Toute modification sur le PC est instantanément visible dans le conteneur. Un volume nommé est géré par Docker et est plus performant mais moins accessible pour l'édition directe.

Suppression du mapping : Airflow ne verrait plus aucun DAG. L'interface serait vide.

Impact multi-nœuds : En production, le dossier /dags doit être identique sur le Scheduler et sur TOUS les Workers. Si un worker n'a pas accès au volume (via un système de fichier partagé comme NFS ou un dépôt Git), la tâche échouera car il ne pourra pas lire le code Python à exécuter.



Q3 — Idempotence et catchup

Catchup=True : Airflow tenterait d'exécuter rétroactivement toutes les instances quotidiennes manquées du 1er janvier 2024 jusqu'à aujourd'hui (plus de 800 runs simultanés !).

Idempotence : Propriété garantissant qu'un DAG produit le même résultat quel que soit le nombre d'exécutions pour une même date. C'est critique pour RTE pour éviter de doubler les statistiques de production en cas de relance d'une tâche.

Rendre les fonctions idempotentes : Utiliser des requêtes API basées sur des plages de dates fixes (ex: ds ou data_interval_start) plutôt que sur "l'heure actuelle" (now()), et s'assurer que l'écriture des données écrase (overwrite) les résultats précédents au lieu de s'ajouter à la suite.


Q4 — Timezone et données temps-réel

Essentiel : RTE gère le réseau français qui suit l'heure de Paris ($UTC+1$ ou $UTC+2$). L'API éCO2mix doit être alignée sur le fuseau local pour corréler correctement la consommation réelle (pic de 19h) avec la météo.

Passage à l'heure d'été : Si mal géré, une heure peut être doublée (en automne) ou manquante (au printemps).

Exemple concret : Fin mars, à 2h du matin il devient 3h. Sans timezone, une requête demandant les données entre 2h et 3h renverra du vide (0 MW produit), faussant totalement les moyennes quotidiennes de production éolienne nocturne.

![alt text](<Screenshot 1 energie meteo dag.png>)
![alt text](<Screenshot 2 energie meteo dag.png>)
![alt text](<Screenshot 3 energie meteo dag.png>)
![alt text](<Screenshot 4 energie meteo dag.png>)
![alt text](<Screenshot 5 energie meteo dag.png>)