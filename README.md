# cassandra-coherence-benchmark
### Cours UQAC 6MIG813 Systèmes Répartis

Reproduction Reproduction de résultats d’évaluation d’une solution mentionnée dans la revue en améliorant la profondeur ou la largeur l’expérimentation 

Article : Consistency in Non-Transactional Distributed Storage Systems
PAOLO VIOTTI,MARKO VUKOLIC

Cette article propose une revue théorique de plus de 50 modèles de cohérence pour les sytémes distribués . Ce projet reproduit et étend empiriquement les résultats de la section 3.8 sur la cohérence tunable déployant un cluster Cassandra réel sur 3 noeuds avec Docker.

Sources : Ce projet est basé sur les dépots suivant
- Déploiement d'un cluster Cassandra avec Docker:
[medvekoma/docker-cassandra](https://github.com/medvekoma/docker-cassandra)
- Connexion Python vers Cassandra dans Docker
[mramshaw/Python_Cassandra](https://github.com/mramshaw/Python_Cassandra)
- Structure générale d'un benchmark de base de données avec Python
[kmjungersen/BenchmarkDB](https://github.com/kmjungersen/BenchmarkDB) 

## Architceture du Cluster 
3 noeuds Cassandra 4.1.11 sur Docker
- cassandra-node1 (seed, port 9042 exposé)
- cassandra-node2
- cassandra-node3
Replication factor : 3
Réseau Docker dédié : cassandra-net



## Niveaux de cohérence testés

| Niveau | Noeuds requis | Modèle dans l'article | Section |
|--------|--------------|----------------------|---------|
| ONE    | 1 sur 3      | Eventual Consistency | 3.2     |
| QUORUM | 2 sur 3      | Tunable Consistency  | 3.8     |
| ALL    | 3 sur 3      | Linearizability      | 3.1     |



## Scripts

| Fichier | Description |
|---------|-------------|
| `benchmark.py` | Benchmark initial — 100 opérations par niveau |
| `benchmark_extended.py` | Extension — charge variable, panne, staleness |
| `README.txt` | Instructions complètes de reproduction |

---

## Résultats — Benchmark initial (100 opérations)

| Niveau | Latence écriture moy. | Latence lecture moy. | Débit écriture | Erreurs |
|--------|-----------------------|----------------------|----------------|---------|
| ONE    | 3.19 ms               | 4.34 ms              | 313 ops/sec    | 0       |
| QUORUM | 4.40 ms               | 7.08 ms              | 227 ops/sec    | 0       |
| ALL    | 73.93 ms              | 4.89 ms              | 13.5 ops/sec   | 7       |

---

## Extension en profondeur — 3 scénarios

**Scénario 1 — Charge variable (10 à 1000 opérations)**
Objectif : observer la stabilité de la latence sous charge croissante.
Observation : effet warm-up constaté, latence diminue sous forte charge.
ALL devient instable à 500 ops avec 4 erreurs WriteTimeout.

**Scénario 2 — Simulation de panne**
Objectif : mesurer la disponibilité de chaque niveau sous stress.
Observation : ONE et QUORUM maintiennent 100% de disponibilité.
ALL génère des timeouts fréquents, illustrant le théorème CAP.

**Scénario 3 — Mesure du staleness**
Objectif : quantifier la fraîcheur des données par niveau.
Lien direct avec la Section 3.6 de l'article : TIMEDVISIBILITY(Δ).
Observation : QUORUM offre le meilleur staleness moyen (3.79 ms).
ONE a le P99 le plus élevé (37.54 ms), illustrant la cohérence éventuelle.

---

## Observations principales

1. Le trade-off cohérence/latence est confirmé empiriquement :
   ALL est 23 fois plus lent que ONE en écriture.

2. Le théorème CAP est illustré concrètement :
   ALL génère 7 erreurs WriteTimeout sur 100 écritures.

3. QUORUM offre le meilleur compromis :
   zéro erreur, latence raisonnable, meilleur staleness moyen.

4. Un effet warm-up est observé sous forte charge :
   phénomène empirique non décrit dans l'article théorique.

---

## Reproduction

Voir `README.txt` pour les instructions complètes.

Prérequis :
- Docker Desktop avec WSL2 et Ubuntu
- Python 3.13
- scylla-driver (alternative à cassandra-driver pour Python 3.13)

```bash
# Déployer le cluster
docker network create cassandra-net
docker run -d --name cassandra-node1 \
  --network cassandra-net \
  -e CASSANDRA_CLUSTER_NAME=TestCluster \
  -e CASSANDRA_DC=datacenter1 \
  -p 9042:9042 cassandra:4.1

# Copier et lancer le benchmark dans le conteneur
docker cp benchmark.py cassandra-node1:/benchmark.py
docker exec -it cassandra-node1 python3 /benchmark.py
```

---

## Note technique

Le driver cassandra-driver officiel est incompatible avec Python 3.13
car le module asyncore a été supprimé dans cette version.
Nous avons utilisé scylla-driver comme alternative compatible.

Les scripts tournent directement dans le conteneur Docker
car la connexion depuis Windows pose des problèmes IPv6
avec les drivers Python.
