BENCHMARK CASSANDRA — COHERENCE TUNABLE
Cours UQAC 6MIG813 — Systemes Repartis
========================================

PREREQUIS
---------
- Docker Desktop installe et demarre
- WSL2 avec Ubuntu installe
- Python 3.13
- Commande : py -m pip install scylla-driver matplotlib pandas numpy

NOTE IMPORTANTE
---------------
Le driver cassandra-driver officiel est incompatible avec Python 3.13
car le module asyncore a ete supprime dans cette version.
Utiliser scylla-driver comme alternative :
py -m pip install scylla-driver

Les scripts tournent directement dans le conteneur Docker
car la connexion depuis Windows pose des problemes IPv6.

ETAPE 1 — Deployer le cluster Cassandra
----------------------------------------
docker network create cassandra-net

docker run -d --name cassandra-node1 --network cassandra-net \
  -e CASSANDRA_CLUSTER_NAME=TestCluster \
  -e CASSANDRA_DC=datacenter1 \
  -e CASSANDRA_RACK=rack1 \
  -p 9042:9042 cassandra:4.1

Attendre 60 secondes puis :

docker run -d --name cassandra-node2 --network cassandra-net \
  -e CASSANDRA_CLUSTER_NAME=TestCluster \
  -e CASSANDRA_DC=datacenter1 \
  -e CASSANDRA_RACK=rack1 \
  -e CASSANDRA_SEEDS=cassandra-node1 cassandra:4.1

Attendre 60 secondes puis :

docker run -d --name cassandra-node3 --network cassandra-net \
  -e CASSANDRA_CLUSTER_NAME=TestCluster \
  -e CASSANDRA_DC=datacenter1 \
  -e CASSANDRA_RACK=rack1 \
  -e CASSANDRA_SEEDS=cassandra-node1 cassandra:4.1

Attendre 90 secondes puis verifier :
docker exec cassandra-node1 nodetool status

Resultat attendu : 3 lignes avec statut UN (Up/Normal)

ETAPE 2 — Creer la base de donnees
------------------------------------
docker exec -it cassandra-node1 cqlsh

Dans le shell CQL :

CREATE KEYSPACE benchmark
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
};

USE benchmark;

CREATE TABLE users (
  id UUID PRIMARY KEY,
  name TEXT,
  value INT,
  timestamp TIMESTAMP
);

EXIT;

ETAPE 3 — Copier et installer les dependances
----------------------------------------------
docker cp benchmark.py cassandra-node1:/benchmark.py
docker cp benchmark_extended.py cassandra-node1:/benchmark_extended.py

docker exec -it cassandra-node1 bash

Dans le conteneur :
apt-get update -q && apt-get install -y python3 python3-pip
pip3 install scylla-driver matplotlib pandas numpy --break-system-packages

ETAPE 4 — Lancer les benchmarks
---------------------------------
Benchmark initial :
python3 /benchmark.py

Benchmark etendu (3 scenarios) :
python3 /benchmark_extended.py

ETAPE 5 — Recuperer les resultats
-----------------------------------
Sortir du conteneur : exit

docker cp cassandra-node1:/benchmark_resultats.png C:\benchmark\benchmark_resultats.png
docker cp cassandra-node1:/benchmark_extended.png C:\benchmark\benchmark_extended.png
docker cp cassandra-node1:/resultats.csv C:\benchmark\resultats.csv
docker cp cassandra-node1:/resultats_charge.csv C:\benchmark\resultats_charge.csv
docker cp cassandra-node1:/resultats_panne.csv C:\benchmark\resultats_panne.csv
docker cp cassandra-node1:/resultats_staleness.csv C:\benchmark\resultats_staleness.csv

FICHIERS DE RESULTATS
----------------------
benchmark_resultats.png  : graphiques benchmark initial (4 graphiques)
benchmark_extended.png   : graphiques benchmark etendu (9 graphiques)
resultats.csv            : donnees ONE/QUORUM/ALL (100 ops)
resultats_charge.csv     : donnees charge variable (10 a 1000 ops)
resultats_panne.csv      : donnees simulation de panne
resultats_staleness.csv  : donnees mesure fraicheur

LIEN AVEC L'ARTICLE
---------------------
ONE    -> Eventual Consistency   (Section 3.2)
QUORUM -> Tunable Consistency    (Section 3.8)
ALL    -> Linearizability approx (Section 3.1)
Staleness -> Delta Consistency   (Section 3.6)

PROBLEMES CONNUS
-----------------
- WriteTimeout au niveau ALL
Normal — illustre le theoreme CAP
ALL exige 3 reponses sur 3 noeuds
Si un noeud est lent : timeout

- Connexion Python depuis Windows impossible
Solution : executer les scripts dans le conteneur Docker
docker exec -it cassandra-node1 python3 /benchmark.py

- cassandra-driver incompatible Python 3.13
Solution : utiliser scylla-driver
pip3 install scylla-driver --break-system-packages
