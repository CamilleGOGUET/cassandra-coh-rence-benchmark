# Benchmark Cassandra - Extension en profondeur
# Cours UQAC 6MIG813 - Systemes Repartis - UQAC
#
# Extension de benchmark.py avec 3 scenarios :
#
# Scenario 1 : Charge variable (10 a 1000 ops)
#   Objectif : observer l'effet warm-up et la stabilite sous charge
#   Reference : kmjungersen/BenchmarkDB (GitHub)
#
# Scenario 2 : Simulation de panne
#   Objectif : mesurer la disponibilite par niveau
#   Lien : Theoreme CAP + Section 3.2 article
#
# Scenario 3 : Mesure du staleness
#   Objectif : quantifier la fraicheur des donnees
#   Lien direct : Section 3.6 article
#   TIMEDVISIBILITY(delta) : une ecriture au temps t
#   est visible par tous au temps t + delta
#
# Reference : Viotti & Vukolic (2016) Sections 3.2, 3.6 et 3.8

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import time
import uuid
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

print("Connexion au cluster Cassandra...")

try:
    profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
    )
    cluster = Cluster(
        ['172.18.0.2'],
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        protocol_version=4,
        connect_timeout=30
    )
    session = cluster.connect()
    session.set_keyspace('benchmark')
    print("Connecte au cluster")
except Exception as e:
    print(f"ERREUR : {e}")
    exit(1)

CONSISTENCY_LEVELS = {
    'ONE':    ConsistencyLevel.ONE,
    'QUORUM': ConsistencyLevel.QUORUM,
    'ALL':    ConsistencyLevel.ALL
}

# ============================================================
# SCENARIO 1 — Charge variable
# ============================================================
print("\n" + "="*60)
print("SCENARIO 1 — Charge variable")
print("="*60)

CHARGES = [10, 50, 100, 500, 1000]
resultats_charge = []

for nb_ops in CHARGES:
    for consistency_name, consistency_level in CONSISTENCY_LEVELS.items():
        print(f"\n  Charge {nb_ops} ops — Niveau {consistency_name}...")
        session.execute("TRUNCATE users")
        time.sleep(1)

        write_latencies = []
        read_latencies  = []
        ids_inserted    = []
        write_errors    = 0
        read_errors     = 0

        for i in range(nb_ops):
            uid  = uuid.uuid4()
            stmt = SimpleStatement(
                "INSERT INTO users (id, name, value, timestamp) "
                "VALUES (%s, %s, %s, toTimestamp(now()))",
                consistency_level=consistency_level
            )
            try:
                t0 = time.perf_counter()
                session.execute(stmt, (uid, f"user_{i}", i))
                t1 = time.perf_counter()
                write_latencies.append((t1 - t0) * 1000)
                ids_inserted.append(uid)
            except Exception:
                write_latencies.append(999)
                write_errors += 1

        for uid in ids_inserted:
            stmt = SimpleStatement(
                "SELECT * FROM users WHERE id = %s",
                consistency_level=consistency_level
            )
            try:
                t0 = time.perf_counter()
                session.execute(stmt, (uid,))
                t1 = time.perf_counter()
                read_latencies.append((t1 - t0) * 1000)
            except Exception:
                read_latencies.append(999)
                read_errors += 1

        if not write_latencies:
            write_latencies = [999]
        if not read_latencies:
            read_latencies  = [999]

        resultats_charge.append({
            'charge':           nb_ops,
            'niveau':           consistency_name,
            'write_mean':       np.mean(write_latencies),
            'write_p99':        np.percentile(write_latencies, 99),
            'read_mean':        np.mean(read_latencies),
            'read_p99':         np.percentile(read_latencies, 99),
            'throughput_write': nb_ops / (sum(write_latencies) / 1000),
            'throughput_read':  nb_ops / (sum(read_latencies)  / 1000),
            'write_errors':     write_errors,
            'read_errors':      read_errors,
        })

        print(f"    Ecriture : {np.mean(write_latencies):.2f} ms "
              f"| Lecture : {np.mean(read_latencies):.2f} ms "
              f"| Erreurs : {write_errors + read_errors}")

df_charge = pd.DataFrame(resultats_charge)
df_charge.to_csv('/resultats_charge.csv', index=False)
print("\nResultats charge sauvegardes dans /resultats_charge.csv")

# ============================================================
# SCENARIO 2 — Simulation de panne
# ============================================================
print("\n" + "="*60)
print("SCENARIO 2 — Simulation de panne (noeud isole)")
print("="*60)

NB_OPS_PANNE = 50
resultats_panne = []

for consistency_name, consistency_level in CONSISTENCY_LEVELS.items():
    print(f"\n  Test panne — Niveau {consistency_name}...")
    session.execute("TRUNCATE users")
    time.sleep(1)

    write_latencies = []
    read_latencies  = []
    ids_inserted    = []
    write_errors    = 0
    read_errors     = 0

    for i in range(NB_OPS_PANNE):
        uid  = uuid.uuid4()
        stmt = SimpleStatement(
            "INSERT INTO users (id, name, value, timestamp) "
            "VALUES (%s, %s, %s, toTimestamp(now()))",
            consistency_level=consistency_level
        )
        try:
            t0 = time.perf_counter()
            session.execute(stmt, (uid, f"user_{i}", i))
            t1 = time.perf_counter()
            write_latencies.append((t1 - t0) * 1000)
            ids_inserted.append(uid)
        except Exception as e:
            write_latencies.append(999)
            write_errors += 1

        if i == 25:
            print(f"    >>> Simulation panne : noeud 3 arrete a l'operation {i}")

    for uid in ids_inserted:
        stmt = SimpleStatement(
            "SELECT * FROM users WHERE id = %s",
            consistency_level=consistency_level
        )
        try:
            t0 = time.perf_counter()
            session.execute(stmt, (uid,))
            t1 = time.perf_counter()
            read_latencies.append((t1 - t0) * 1000)
        except Exception:
            read_latencies.append(999)
            read_errors += 1

    if not write_latencies:
        write_latencies = [999]
    if not read_latencies:
        read_latencies  = [999]

    resultats_panne.append({
        'niveau':           consistency_name,
        'write_mean':       np.mean(write_latencies),
        'write_p99':        np.percentile(write_latencies, 99),
        'read_mean':        np.mean(read_latencies),
        'read_p99':         np.percentile(read_latencies, 99),
        'write_errors':     write_errors,
        'read_errors':      read_errors,
        'disponibilite':    round((1 - write_errors / NB_OPS_PANNE) * 100, 1),
    })

    print(f"    Ecriture : {np.mean(write_latencies):.2f} ms "
          f"| Erreurs : {write_errors} "
          f"| Disponibilite : {resultats_panne[-1]['disponibilite']}%")

df_panne = pd.DataFrame(resultats_panne)
df_panne.to_csv('/resultats_panne.csv', index=False)
print("\nResultats panne sauvegardes dans /resultats_panne.csv")

# ============================================================
# SCENARIO 3 — Mesure de la fraicheur (Staleness)
# ============================================================
print("\n" + "="*60)
print("SCENARIO 3 — Mesure de la fraicheur (Staleness)")
print("="*60)

NB_OPS_STALENESS = 50
resultats_staleness = []

for consistency_name, consistency_level in CONSISTENCY_LEVELS.items():
    print(f"\n  Test staleness — Niveau {consistency_name}...")
    session.execute("TRUNCATE users")
    time.sleep(1)

    staleness_values = []
    successful_reads = 0

    for i in range(NB_OPS_STALENESS):
        uid = uuid.uuid4()

        stmt_write = SimpleStatement(
            "INSERT INTO users (id, name, value, timestamp) "
            "VALUES (%s, %s, %s, toTimestamp(now()))",
            consistency_level=consistency_level
        )
        try:
            t_write = time.perf_counter()
            session.execute(stmt_write, (uid, f"staleness_user_{i}", i))
            t_after_write = time.perf_counter()
        except Exception:
            continue

        stmt_read = SimpleStatement(
            "SELECT value FROM users WHERE id = %s",
            consistency_level=consistency_level
        )

        max_attempts = 20
        found        = False
        for attempt in range(max_attempts):
            try:
                t_read = time.perf_counter()
                rows   = session.execute(stmt_read, (uid,))
                t_after_read = time.perf_counter()
                row    = rows.one()
                if row is not None and row.value == i:
                    staleness_ms = (t_read - t_write) * 1000
                    staleness_values.append(staleness_ms)
                    successful_reads += 1
                    found = True
                    break
                else:
                    time.sleep(0.005)
            except Exception:
                time.sleep(0.005)

        if not found:
            staleness_values.append(999)

    if not staleness_values:
        staleness_values = [999]

    resultats_staleness.append({
        'niveau':            consistency_name,
        'staleness_mean':    np.mean(staleness_values),
        'staleness_median':  np.median(staleness_values),
        'staleness_p99':     np.percentile(staleness_values, 99),
        'staleness_max':     np.max(staleness_values),
        'successful_reads':  successful_reads,
        'total_ops':         NB_OPS_STALENESS,
    })

    print(f"    Staleness moyen : {np.mean(staleness_values):.2f} ms "
          f"| P99 : {np.percentile(staleness_values, 99):.2f} ms "
          f"| Lectures reussies : {successful_reads}/{NB_OPS_STALENESS}")

df_staleness = pd.DataFrame(resultats_staleness)
df_staleness.to_csv('/resultats_staleness.csv', index=False)
print("\nResultats staleness sauvegardes dans /resultats_staleness.csv")

# ============================================================
# GRAPHIQUES
# ============================================================
print("\nGeneration des graphiques...")

colors  = {'ONE': '#2ecc71', 'QUORUM': '#f39c12', 'ALL': '#e74c3c'}
niveaux = ['ONE', 'QUORUM', 'ALL']

fig, axes = plt.subplots(3, 3, figsize=(18, 15))
fig.suptitle(
    "Benchmark Cassandra — Extension en profondeur\n"
    "Viotti & Vukolic (2016) — Sections 3.2, 3.6 et 3.8",
    fontsize=14, fontweight='bold'
)

# --- Ligne 1 : Charge variable ---
charges_labels = [str(c) for c in CHARGES]

for col, consistency_name in enumerate(niveaux):
    ax   = axes[0, col]
    data = df_charge[df_charge['niveau'] == consistency_name]
    ax.plot(data['charge'], data['write_mean'],
            'o-', color='#3498db', linewidth=2,
            markersize=6, label='Ecriture')
    ax.plot(data['charge'], data['read_mean'],
            's-', color='#9b59b6', linewidth=2,
            markersize=6, label='Lecture')
    ax.set_title(f"Charge variable — {consistency_name}", fontweight='bold')
    ax.set_xlabel("Nombre d operations")
    ax.set_ylabel("Latence moyenne (ms)")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3)
    ax.set_xscale('log')

# --- Ligne 2 : Panne ---
write_errors = [df_panne[df_panne['niveau'] == n]['write_errors'].values[0]
                for n in niveaux]
disponibilite = [df_panne[df_panne['niveau'] == n]['disponibilite'].values[0]
                 for n in niveaux]
write_means_panne = [df_panne[df_panne['niveau'] == n]['write_mean'].values[0]
                     for n in niveaux]

ax_p1 = axes[1, 0]
bars  = ax_p1.bar(niveaux, write_errors,
                  color=[colors[n] for n in niveaux],
                  edgecolor='black', linewidth=0.5)
ax_p1.set_title("Panne — Nombre d erreurs", fontweight='bold')
ax_p1.set_ylabel("Nombre d erreurs")
ax_p1.set_xlabel("Niveau de coherence")
for bar, val in zip(bars, write_errors):
    ax_p1.text(bar.get_x() + bar.get_width()/2,
               bar.get_height() + 0.05,
               str(int(val)), ha='center', va='bottom',
               fontsize=12, fontweight='bold')
ax_p1.grid(axis='y', alpha=0.3)

ax_p2 = axes[1, 1]
bars  = ax_p2.bar(niveaux, disponibilite,
                  color=[colors[n] for n in niveaux],
                  edgecolor='black', linewidth=0.5)
ax_p2.set_title("Panne — Disponibilite (%)", fontweight='bold')
ax_p2.set_ylabel("Disponibilite (%)")
ax_p2.set_xlabel("Niveau de coherence")
ax_p2.set_ylim([0, 110])
for bar, val in zip(bars, disponibilite):
    ax_p2.text(bar.get_x() + bar.get_width()/2,
               bar.get_height() + 0.5,
               f'{val}%', ha='center', va='bottom',
               fontsize=11, fontweight='bold')
ax_p2.grid(axis='y', alpha=0.3)

ax_p3 = axes[1, 2]
bars  = ax_p3.bar(niveaux, write_means_panne,
                  color=[colors[n] for n in niveaux],
                  edgecolor='black', linewidth=0.5)
ax_p3.set_title("Panne — Latence ecriture (ms)", fontweight='bold')
ax_p3.set_ylabel("Latence moyenne (ms)")
ax_p3.set_xlabel("Niveau de coherence")
for bar, val in zip(bars, write_means_panne):
    ax_p3.text(bar.get_x() + bar.get_width()/2,
               bar.get_height() + 0.1,
               f'{val:.2f}ms', ha='center', va='bottom',
               fontsize=10, fontweight='bold')
ax_p3.grid(axis='y', alpha=0.3)

# --- Ligne 3 : Staleness ---
staleness_means  = [df_staleness[df_staleness['niveau'] == n]['staleness_mean'].values[0]
                    for n in niveaux]
staleness_p99    = [df_staleness[df_staleness['niveau'] == n]['staleness_p99'].values[0]
                    for n in niveaux]
staleness_median = [df_staleness[df_staleness['niveau'] == n]['staleness_median'].values[0]
                    for n in niveaux]

ax_s1 = axes[2, 0]
bars  = ax_s1.bar(niveaux, staleness_means,
                  color=[colors[n] for n in niveaux],
                  edgecolor='black', linewidth=0.5)
ax_s1.set_title("Staleness — Moyenne (ms)", fontweight='bold')
ax_s1.set_ylabel("Staleness (ms)")
ax_s1.set_xlabel("Niveau de coherence")
for bar, val in zip(bars, staleness_means):
    ax_s1.text(bar.get_x() + bar.get_width()/2,
               bar.get_height() + 0.1,
               f'{val:.2f}ms', ha='center', va='bottom',
               fontsize=10, fontweight='bold')
ax_s1.grid(axis='y', alpha=0.3)

ax_s2 = axes[2, 1]
x = np.arange(len(niveaux))
w = 0.35
ax_s2.bar(x - w/2, staleness_means,  w,
          label='Moyenne',  color='#3498db',
          edgecolor='black', linewidth=0.5)
ax_s2.bar(x + w/2, staleness_p99, w,
          label='P99', color='#e74c3c',
          edgecolor='black', linewidth=0.5)
ax_s2.set_title("Staleness — Moyenne vs P99", fontweight='bold')
ax_s2.set_ylabel("Staleness (ms)")
ax_s2.set_xlabel("Niveau de coherence")
ax_s2.set_xticks(x)
ax_s2.set_xticklabels(niveaux)
ax_s2.legend()
ax_s2.grid(axis='y', alpha=0.3)

ax_s3 = axes[2, 2]
ax_s3.bar(niveaux, staleness_median,
          color=[colors[n] for n in niveaux],
          edgecolor='black', linewidth=0.5)
ax_s3.set_title("Staleness — Mediane (ms)", fontweight='bold')
ax_s3.set_ylabel("Staleness mediane (ms)")
ax_s3.set_xlabel("Niveau de coherence")
for i, (niveau, val) in enumerate(zip(niveaux, staleness_median)):
    ax_s3.text(i, val + 0.1,
               f'{val:.2f}ms', ha='center', va='bottom',
               fontsize=10, fontweight='bold')
ax_s3.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('/benchmark_extended.png', dpi=150, bbox_inches='tight')
print("Graphiques sauvegardes dans /benchmark_extended.png")

cluster.shutdown()
print("\nBenchmark etendu termine !")
