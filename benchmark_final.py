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

print("Etape 1 - Imports OK")

try:
    profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
    )
    print("Etape 2 - Profile cree")

    cluster = Cluster(
        ['172.18.0.2'],
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        protocol_version=4,
        connect_timeout=30
    )
    print("Etape 3 - Cluster cree")

    session = cluster.connect()
    print("Etape 4 - Connecte au cluster")

    session.set_keyspace('benchmark')
    print("Etape 5 - Keyspace selectionne")

except Exception as e:
    print(f"ERREUR : {type(e).__name__}")
    print(f"Detail : {e}")
    import traceback
    traceback.print_exc()
    exit(1)

CONSISTENCY_LEVELS = {
    'ONE':    ConsistencyLevel.ONE,
    'QUORUM': ConsistencyLevel.QUORUM,
    'ALL':    ConsistencyLevel.ALL
}

NB_OPERATIONS = 100

def benchmark(consistency_name, consistency_level, nb_ops=NB_OPERATIONS):
    print(f"\n{'='*50}")
    print(f"Test niveau : {consistency_name}")
    print(f"{'='*50}")

    write_latencies = []
    read_latencies  = []
    ids_inserted    = []
    write_errors    = 0
    read_errors     = 0

    # ECRITURES
    print(f"  Ecriture de {nb_ops} entrees...")
    for i in range(nb_ops):
        uid  = uuid.uuid4()
        stmt = SimpleStatement(
            "INSERT INTO users (id, name, value, timestamp) VALUES (%s, %s, %s, toTimestamp(now()))",
            consistency_level=consistency_level
        )
        try:
            t0 = time.perf_counter()
            session.execute(stmt, (uid, f"user_{i}", i))
            t1 = time.perf_counter()
            latency_ms = (t1 - t0) * 1000
            write_latencies.append(latency_ms)
            ids_inserted.append(uid)
        except Exception as e:
            print(f"  Ecriture echouee : {type(e).__name__}")
            write_latencies.append(999)
            write_errors += 1

    # LECTURES
    print(f"  Lecture de {nb_ops} entrees...")
    for uid in ids_inserted:
        stmt = SimpleStatement(
            "SELECT * FROM users WHERE id = %s",
            consistency_level=consistency_level
        )
        try:
            t0 = time.perf_counter()
            session.execute(stmt, (uid,))
            t1 = time.perf_counter()
            latency_ms = (t1 - t0) * 1000
            read_latencies.append(latency_ms)
        except Exception as e:
            print(f"  Lecture echouee : {type(e).__name__}")
            read_latencies.append(999)
            read_errors += 1

    if not write_latencies:
        write_latencies = [999]
    if not read_latencies:
        read_latencies = [999]

    results = {
        'niveau':           consistency_name,
        'write_mean':       np.mean(write_latencies),
        'write_median':     np.median(write_latencies),
        'write_p99':        np.percentile(write_latencies, 99),
        'write_max':        np.max(write_latencies),
        'write_errors':     write_errors,
        'read_mean':        np.mean(read_latencies),
        'read_median':      np.median(read_latencies),
        'read_p99':         np.percentile(read_latencies, 99),
        'read_max':         np.max(read_latencies),
        'read_errors':      read_errors,
        'throughput_write': nb_ops / (sum(write_latencies) / 1000),
        'throughput_read':  nb_ops / (sum(read_latencies)  / 1000),
    }

    print(f"\n  Resultats {consistency_name} :")
    print(f"  Ecriture - Moyenne : {results['write_mean']:.2f} ms | P99 : {results['write_p99']:.2f} ms | Debit : {results['throughput_write']:.1f} ops/sec | Erreurs : {write_errors}")
    print(f"  Lecture  - Moyenne : {results['read_mean']:.2f} ms  | P99 : {results['read_p99']:.2f} ms  | Debit : {results['throughput_read']:.1f} ops/sec  | Erreurs : {read_errors}")

    return results, write_latencies, read_latencies

# Lancer tous les tests
all_results   = []
all_write_lat = {}
all_read_lat  = {}

for name, level in CONSISTENCY_LEVELS.items():
    session.execute("TRUNCATE users")
    time.sleep(2)
    res, w_lat, r_lat = benchmark(name, level)
    all_results.append(res)
    all_write_lat[name] = w_lat
    all_read_lat[name]  = r_lat

# Sauvegarder CSV
df = pd.DataFrame(all_results)
df.to_csv('/resultats.csv', index=False)
print("\nResultats sauvegardes dans /resultats.csv")

# Graphiques
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle(
    "Benchmark Cassandra - Coherence Tunable\nViotti & Vukolic (2016) - Section 3.8",
    fontsize=14, fontweight='bold'
)

colors  = {'ONE': '#2ecc71', 'QUORUM': '#f39c12', 'ALL': '#e74c3c'}
niveaux = [r['niveau'] for r in all_results]

# Graphique 1 - Latence ecriture
ax1  = axes[0, 0]
vals = [r['write_mean'] for r in all_results]
bars = ax1.bar(niveaux, vals,
               color=[colors[n] for n in niveaux],
               edgecolor='black', linewidth=0.5)
ax1.set_title("Latence moyenne - Ecriture", fontweight='bold')
ax1.set_ylabel("Latence (ms)")
ax1.set_xlabel("Niveau de coherence")
for bar, val in zip(bars, vals):
    ax1.text(bar.get_x() + bar.get_width()/2,
             bar.get_height() + 0.1,
             f'{val:.2f}ms', ha='center', va='bottom',
             fontsize=10, fontweight='bold')
ax1.grid(axis='y', alpha=0.3)

# Graphique 2 - Latence lecture
ax2  = axes[0, 1]
vals = [r['read_mean'] for r in all_results]
bars = ax2.bar(niveaux, vals,
               color=[colors[n] for n in niveaux],
               edgecolor='black', linewidth=0.5)
ax2.set_title("Latence moyenne - Lecture", fontweight='bold')
ax2.set_ylabel("Latence (ms)")
ax2.set_xlabel("Niveau de coherence")
for bar, val in zip(bars, vals):
    ax2.text(bar.get_x() + bar.get_width()/2,
             bar.get_height() + 0.1,
             f'{val:.2f}ms', ha='center', va='bottom',
             fontsize=10, fontweight='bold')
ax2.grid(axis='y', alpha=0.3)

# Graphique 3 - Debit
ax3 = axes[1, 0]
x   = np.arange(len(niveaux))
w   = 0.35
ax3.bar(x - w/2, [r['throughput_write'] for r in all_results],
        w, label='Ecriture', color='#3498db',
        edgecolor='black', linewidth=0.5)
ax3.bar(x + w/2, [r['throughput_read'] for r in all_results],
        w, label='Lecture', color='#9b59b6',
        edgecolor='black', linewidth=0.5)
ax3.set_title("Debit (Throughput)", fontweight='bold')
ax3.set_ylabel("Operations / seconde")
ax3.set_xlabel("Niveau de coherence")
ax3.set_xticks(x)
ax3.set_xticklabels(niveaux)
ax3.legend()
ax3.grid(axis='y', alpha=0.3)

# Graphique 4 - Boxplot
ax4  = axes[1, 1]
data = [all_read_lat[n] for n in niveaux]
bp   = ax4.boxplot(data, labels=niveaux, patch_artist=True)
for patch, name in zip(bp['boxes'], niveaux):
    patch.set_facecolor(colors[name])
    patch.set_alpha(0.7)
ax4.set_title("Distribution latences - Lecture", fontweight='bold')
ax4.set_ylabel("Latence (ms)")
ax4.set_xlabel("Niveau de coherence")
ax4.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('/benchmark_resultats.png', dpi=150, bbox_inches='tight')
print("Graphiques sauvegardes dans /benchmark_resultats.png")

cluster.shutdown()
print("\nBenchmark termine !")
