# Réponses TP Jour 2

## Q1 — HDFS vs système de fichiers local
[3 avantages concrets de HDFS pour 50 Go/jour :

Distribution : Les données sont réparties sur plusieurs serveurs, permettant de stocker plus que la capacité d'un seul disque local.

Réplication : Chaque bloc est copié sur plusieurs DataNodes (par défaut 3), assurant la tolérance aux pannes. Si un serveur tombe, les données restent accessibles.

Localité des données : Le calcul (MapReduce, Spark) est déplacé vers les nœuds où résident les données, évitant les transferts réseau massifs et améliorant les performances.]

## Q2 — NameNode SPOF
[Si le NameNode tombe :

Les DataNodes continuent de fonctionner mais deviennent inutilisables pour les clients

Plus aucune opération de lecture/écriture n'est possible

Le système HDFS est totalement bloqué

Mécanismes HA (High Availability) en production :

NameNode HA : Deux NameNodes (actif + standby) partagent l'état via des JournalNodes

JournalNodes : Ensemble de nœuds (minimum 3) qui loggent les transactions d'édition. Le NameNode actif écrit les modifications, le standby les lit pour rester synchronisé.

Failover automatique via Zookeeper en cas de panne]

## Q3 — HdfsSensor : poke vs reschedule
[Mode	Comportement	Impact workers
poke	Le worker reste actif et vérifie périodiquement	Bloque un slot worker pendant toute l'attente
reschedule	Le worker se libère entre les vérifications	Libère le slot, un autre worker reprend après l'intervalle
Quand utiliser :

poke : Attente courte (< 1 minute) ou ressources workers illimitées

reschedule : Attente longue (fichier pouvant arriver dans plusieurs heures) → recommandé en production

Scénario bloquant : Si vous avez 4 workers et 5 DAGs avec des sensors en poke attendant 1 heure chacun, tous les workers sont bloqués → plus aucun DAG ne peut s'exécuter.]

## Q4 — Réplication HDFS
[Écriture d'un bloc de 128 Mo :

Le client écrit la première copie sur le DataNode local (même rack)

Le 1er DataNode réplique sur un 2e DataNode (rack différent si configuré)

Le 2e DataNode réplique sur un 3e DataNode

Total : 3 copies sur 3 DataNodes distincts

Cohérence lors d'une lecture concurrente :

HDFS garantit la cohérence à la lecture : si un client lit pendant qu'un autre écrit, il verra soit l'ancienne version complète, soit la nouvelle version complète

Pas de lecture partielle de bloc en cours d'écriture

Le bloc est considéré comme "committed" seulement après la réplication complète sur tous les DataNodes

Utilisation des leases (baux) : un seul client peut écrire sur un fichier à la fois]
