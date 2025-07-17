"""Benchmark text search using the custom inverted index and PostgreSQL.

This script compares search speed and result quality between ``InvertedIndex``
and PostgreSQL full text search.  Two ranking functions are measured for
PostgreSQL: ``ts_rank`` and ``ts_rank_cd``.
"""

import csv
import json
import os
import re
import time
from pathlib import Path

import psycopg2

from indexes.invertedindex import InvertedIndex
from preprocessing.text import processingDatasetOnInvertedFile
from preprocessing.text_utils import bagOfWords
from core.text_file import TextFile

# Reuse preprocessing files if they already exist
REUSE_INDEX = True
# Avoid regenerating TextFile preprocessing if data files already exist
REUSE_TEXT = True
# Enable PostgreSQL benchmarking
RUN_POSTGRES = True
# Use websearch_to_tsquery instead of plainto_tsquery when COMPARE_BOTH_MODES
# is False.  When ``COMPARE_BOTH_MODES`` is True both modes are benchmarked.
USE_WEBSEARCH = False
# Benchmark both ``plainto_tsquery`` and ``websearch_to_tsquery`` modes
COMPARE_BOTH_MODES = True

# Custom sizes can override the default benchmarking scale
CUSTOM_SIZES: list[int] | None = []
JSON_FILE = "benchmark_results_full.json"
PLOTS_DIR = "benchmark_plots"

# Different top-k values used when computing Jaccard metrics
TOP_K_VALUES = [5, 10, 20, 50, 100]

# List of tsquery functions to benchmark. A custom mode ``custom_bow``
# converts the query using ``bagOfWords`` and joins the tokens with
# ``|`` to approximate the inverted index behaviour.
if COMPARE_BOTH_MODES:
    QUERY_FUNCS = [
        "plainto_tsquery",
        "websearch_to_tsquery",
        "custom_bow",
    ]
else:
    QUERY_FUNCS = [
        "websearch_to_tsquery" if USE_WEBSEARCH else "plainto_tsquery",
        "custom_bow",
    ]


# Dataset sizes used for each benchmark iteration. These will be
# trimmed to the dataset length at runtime so they never exceed the
# number of available rows.
DEFAULT_SIZES = [1000, 2000, 4000, 6000, 8000, 10000, 14000]

# Fixed queries for measuring search performance
QUERIES = [
    "jurassic park",
    "the matrix",
    "romantic comedy",
    "space adventure",
    "serial killer",
    "time travel",
    "family drama",
    "world war ii",
    "alien invasion",
    "superhero origin",
    "The Beatles"
    "rock and roll"
"minecraft fortnite skibidi among us tiktok blockchain NFT",
    "cryptocurrency influencer AI art deepfake meme culture vending",
    "y2k cosplay drone streaming e-girl vaporwave hypetrain",
]

# Additional edge-case queries
ALL_QUERIES = QUERIES + [
# Fragmento completo de un documento
    """Giorgio (Glauco Onorato), who explains that the knife belongs to his father, who has not been seen for five days. Giorgio offers a room to the young count, and subsequently introduces him to the rest of the family: his wife (Rika Dialina), their young son Ivan, Giorgio's younger brother Pietro (Massimo Righi), and sister Sdenka (Susy Anderson). It subsequently transpires that they are eagerly anticipating the arrival of their father, Gorcha, as well as the reason for his absence: he has gone to do battle with the outlaw and dreaded wurdalak Ali Beg. Vladimir is confused by the term, and Sdenka explains that a wurdalak is a walking cadaver who feeds on the blood of the living, preferably close friends and family members. Giorgio and Pietro are certain that the corpse Vladimir had discovered is that of Ali Beg, but also realize that there is a strong possibility that their father has been infected by the blood curse too. They warn the count to leave, but he decides to stay and await the old mans return. At the stroke of midnight, Gorcha (Boris Karloff) returns to the cottage. His sour demeanor and unkempt appearance bode the worse, and the two brothers are torn: they realize that it is their duty to kill Gorcha before he feeds on the family, but their love for him makes it difficult to reach a decision. Later that night, both Ivan and Pietro are attacked by Gorcha who drains them of blood, and then flees the cottage. Giorgio stakes and beheads Pietro to prevent him from reviving as a wurdalak. But he is prevented from doing so to Ivan when his wife threatens to commit suicide. Reluctantly, he agrees to bury the child without taking the necessary precautions. That same night, the child rises from his grave and begs to be invited into the cottage. The mother runs to her son's aid, stabbing Giorgio when he attempts to stop her, only to be greeted at the front door by Gorcha. The old man bites and infects his daughter-in-law, who then does the same for her husband. Vladimir and Sdenka flee from the cottage and go on the run and hide out in the ruins of an abandoned cathedral as dawn breaks. Vladimir is optimistic that a long and happy life lies with them. But Sdenka is reluctant to relinquish her family ties. She believes that she is meant to stay with the family. Sdenka's fears about her family are confirmed when that evening, Gorcha and her siblings show up at the abandoned Abbey. As Vladimir sleeps, Sdenka is lured into their loving arms where they bite to death. Awakened by her screams, Vladimir rushes to her aid, but the family has already taken her home, forcing the lover to follow suit.""",

    # Fragmentos combinados de distintos documentos
    """s of the New Sun swore an oath to resurrect hope in the land. The purity of their hearts was so great that Pelor, the God of Light, gave the Knights powerful amulets with which to channel his power. Transcendent with divine might, the Knights of the New Sun pierced the shadow that had darkened the land for twelve hundred years and cast it asunder. But not all were awed by their glory. The disciples of Nhagruul disassembled the book and bribed three greedy souls to hide the pieces until they could be retrieved. The ink was discovered and destroyed but, despite years of searching, the cover and pages were never found. Peace ruled the land for centuries and the Knights got lost in the light of their own glory. As memory of the awful events faded so did the power of servants of Pelor.
    Klara is there waiting for him, with the chosen book and wearing a red carnation they'd agreed to use as a signal. Realizing that he'd been wrong about her all along, and that his irritation with her was actually masking his attraction, he finally enters and goes over to her table, but does not reveal his true reason for being there although he is aware she will be hurt that her pen pal doesn't show up.
    Glenn sacrificing his summer vacation, which he intended to use to work on his composing, in order to make extra money teaching Driver's Ed. Glenn does right by his family but he knows he can forget about getting out of the teaching gig for the foreseeable future. Continuing his new, unorthodox teaching methods, he finally gets Gertrude, who was on the verge of giving up, to have a breakthrough and become a more skilled clarinet player. She rediscovers her joy of playing, and the now-competent band go on to play at the 1965 graduation. Summer vacation begins, and Glenn follows through on his plan to teach Driver's Ed, having a series of near-death experiences at the hands of new drivers. Glenn and Iris move into their new house. Soon, we see the Driver's Ed car once again, except this time it is Glenn himself driving like a maniac, breaking every traffic law – so that he could get to the hospital to see his newborn son, Coltrane ("Cole").""",
]

# Sizes used when running the optional GiST experiment
GIST_SIZES = [1000, 2000, 4000, 6000, 8000, 10000, 14000]
RUN_GIST = True

# Path to the full dataset
DATASET_PATH = Path("datasets/data2/mpst_full_data.csv")
# Temporary directory for generated CSV files
TMP_DIR = Path(".")

# Determine the total number of rows in the dataset (excluding header)


def dataset_row_count(path: Path) -> int:
    """Return the number of documents available in ``path``."""
    with open(path, newline="", encoding="utf-8") as f:
        # ``csv.reader`` correctly handles newlines within quoted fields so
        # counting rows this way yields the true number of records.  Subtract
        # one to exclude the header row.
        return sum(1 for _ in csv.reader(f)) - 1


DATASET_ROWS = dataset_row_count(DATASET_PATH)


def bow_tsquery(text: str) -> str:
    """Return a tsquery string using bag-of-words tokenization."""
    tokens = bagOfWords(text).keys()
    return " | ".join(tokens)


def build_sizes() -> list[int]:
    """Return the benchmark sizes limited to the dataset length."""
    base = CUSTOM_SIZES or DEFAULT_SIZES
    sizes = sorted(set(n for n in base if n <= DATASET_ROWS))
    if DATASET_ROWS not in sizes:
        if not sizes or sizes[-1] < DATASET_ROWS:
            sizes.append(DATASET_ROWS)
    return sizes


SIZES = build_sizes()
print(SIZES)

# Connection parameters for the PostgreSQL instance
PG_PARAMS = {
    "host": os.environ.get("PG_HOST", "localhost"),
    "port": os.environ.get("PG_PORT", "5432"),
    "dbname": os.environ.get("PG_DB", "postgres"),
    "user": os.environ.get("PG_USER", "postgres"),
    "password": os.environ.get("PG_PASSWORD", "postgres"),
}


def create_tmp_csv(n: int) -> Path:
    """Create or reuse a temporary CSV with the first ``n`` rows."""
    tmp_csv = TMP_DIR / f"tmp_mpst_{n}.csv"
    if REUSE_INDEX and tmp_csv.exists():
        return tmp_csv

    with (
        open(DATASET_PATH, newline="", encoding="utf-8") as src,
        open(tmp_csv, "w", newline="", encoding="utf-8") as dst,
    ):
        reader = csv.DictReader(src)
        writer = csv.DictWriter(dst, fieldnames=["title", "plot_synopsis"])
        writer.writeheader()
        for i, row in enumerate(reader):
            if i >= n:
                break
            writer.writerow(
                {"title": row["title"], "plot_synopsis": row["plot_synopsis"]}
            )
    return tmp_csv


def ensure_text_files(csv_path: Path) -> str:
    """Generate TextFile data for a CSV if missing."""
    tf = TextFile(str(csv_path))
    if not (REUSE_TEXT and Path(tf.data_path).exists()):
        tf.initialize()
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                tf.write(row["plot_synopsis"])
    return str(csv_path)


def cache_index(csv_path: Path) -> str:
    """Return or build the inverted index for ``csv_path``."""
    base = str(csv_path)
    paths = [
        Path(base + suffix)
        for suffix in ("_inv.dat", "_doc.dat", "_data.dat", "_lengths.dat")
    ]

    if REUSE_INDEX and all(p.exists() for p in paths):
        return paths[0].as_posix()

    ensure_text_files(csv_path)
    return processingDatasetOnInvertedFile(base)


def cleanup_index(csv_path: Path) -> None:
    """Remove temporary files generated for ``csv_path``."""
    base = csv_path.name
    paths = [
        csv_path,
        csv_path.parent / f"{base}_inv.dat",
        csv_path.parent / f"{base}_doc.dat",
        csv_path.parent / f"{base}_data.dat",
        csv_path.parent / f"{base}_lengths.dat",
        csv_path.parent / f"{base}_lengths.dat_lengths.dat",
    ]
    for path in paths:
        if path.exists():
            path.unlink()


def benchmark_myindex(
    n: int, queries: list[str], top_k: int
) -> tuple[float, list[list[str]], list[list[float]]]:
    """Build ``InvertedIndex`` and return timings, IDs and scores."""
    tmp_csv = create_tmp_csv(n)
    index_path = cache_index(tmp_csv)
    idx = InvertedIndex(index_path)
    built_flag = Path(index_path + ".built")
    if not (REUSE_INDEX and built_flag.exists()):
        idx.buildIndex()
        built_flag.touch()

    timings: list[float] = []
    ids_res: list[list[str]] = []
    sims_res: list[list[float]] = []
    for q in queries:
        res = None
        start = time.perf_counter()
        for _ in range(5):
            res = idx.searchQuery(q, limit=top_k)
        timings.append((time.perf_counter() - start) / 5)
        ids = [doc_id for doc_id, _ in res or []]
        sims = [score for _, score in res or []]
        ids_res.append(ids)
        sims_res.append(sims)

    avg_ms = sum(timings) / len(timings) * 1000

    if not REUSE_INDEX:
        cleanup_index(tmp_csv)

    return avg_ms, ids_res, sims_res


def parse_execution_time(plan_rows: list[tuple[str]]) -> float:
    """Extract execution time from ``EXPLAIN ANALYZE`` output."""
    text = "\n".join(r[0] for r in plan_rows)
    match = re.search(r"Execution Time: ([0-9.]+) ms", text)
    return float(match.group(1)) if match else 0.0


def compute_quality(
    my_res: list[list[str]],
    pg_res: list[list[str]],
    ks: list[int],
) -> dict[int, dict[str, list | float]]:
    """Return quality metrics for each ``k`` value."""
    metrics: dict[int, dict[str, list | float]] = {}
    for k in ks:
        exact_list: list[bool] = []
        jacc_list: list[float] = []
        for m, p in zip(my_res, pg_res):
            s1, s2 = set(m[:k]), set(p[:k])
            exact_list.append(s1 == s2)
            union = len(s1 | s2)
            inter = len(s1 & s2)
            jacc_list.append(inter / union if union else 1.0)
        exact_avg = sum(1.0 for e in exact_list if e) / len(exact_list)
        jacc_avg = sum(jacc_list) / len(jacc_list)
        metrics[k] = {
            "exact_avg": exact_avg,
            "jacc_avg": jacc_avg,
            "exact_list": exact_list,
            "jacc_list": jacc_list,
        }
    return metrics


def benchmark_postgres(
    n: int,
    queries: list[str],
    conn,
    ranking: str = "ts_rank",
    query_func: str = "plainto_tsquery",
    use_gist: bool = False,
    top_k: int = max(TOP_K_VALUES),
) -> tuple[float, list[list[str]], list[list[float]]]:
    """Load data into PostgreSQL and run text search.

    Returns timing in milliseconds, list of document IDs and their ranks.
    """
    tmp_csv = create_tmp_csv(n)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS movies")
    cur.execute(
        "CREATE TABLE movies("
        "id serial PRIMARY KEY, title text, plot_synopsis text)"
    )
    # Bulk load the temporary CSV into the database
    with open(tmp_csv, "r", encoding="utf-8") as f:
        cur.copy_expert(
            "COPY movies(title, plot_synopsis) FROM STDIN WITH CSV HEADER",
            f,
        )
    cur.execute(
        "ALTER TABLE movies ADD COLUMN document_with_weights tsvector"
    )
    cur.execute(
        "UPDATE movies SET document_with_weights = "
        "to_tsvector('english', plot_synopsis)"
    )
    index_type = "GiST" if use_gist else "GIN"
    cur.execute(
        (
            "CREATE INDEX idx_movies_fts ON movies USING "
            f"{index_type}(document_with_weights)"

        )
    )
    conn.commit()

    timings: list[float] = []
    ids_res: list[list[str]] = []
    ranks_res: list[list[float]] = []
    for q in queries:
        func = query_func
        prepared = q
        if query_func == "custom_bow":
            func = "to_tsquery"
            prepared = bow_tsquery(q)
        total = 0.0
        for _ in range(5):
            cur.execute(
                f"""EXPLAIN (ANALYZE, BUFFERS)
                SELECT id,
                       {ranking}(document_with_weights,
                                {func}('english', %(q)s)) AS rank
                FROM movies
                WHERE document_with_weights @@ {func}('english', %(q)s)
                ORDER BY rank DESC
                LIMIT {top_k}
                """,
                {"q": prepared},
            )
            plan = cur.fetchall()
            total += parse_execution_time(plan)
        timings.append(total / 5)

        cur.execute(
            f"""SELECT id,
                       {ranking}(document_with_weights,
                               {func}('english', %(q)s)) AS rank
                FROM movies
                WHERE document_with_weights @@ {func}('english', %(q)s)
                ORDER BY rank DESC
                LIMIT {top_k}""",
            {"q": prepared},
        )
        rows = cur.fetchall()
        ids_res.append([str(r[0] - 1) for r in rows])
        ranks_res.append([float(r[1]) for r in rows])
    avg_ms = sum(timings) / len(timings)

    cur.close()
    if not REUSE_INDEX:
        cleanup_index(tmp_csv)

    return avg_ms, ids_res, ranks_res


def save_full_results(data: list[dict], path: str) -> None:
    """Persist the extended benchmark results."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print a Markdown table to stdout."""
    print("| " + " | ".join(headers) + " |")
    print("| " + " | ".join("-" * len(h) for h in headers) + " |")
    for row in rows:
        print("| " + " | ".join(row) + " |")


def write_csv(path: str, headers: list[str], rows: list[list[str]]) -> None:
    """Save table rows to a CSV file."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


def generate_outputs(
    results: list[list[float | int]],
    full: list[dict],
    entry_map: dict[int, dict],
    save_files: bool = True,
    output_dir: str = PLOTS_DIR,
) -> None:
    """Print tables, optionally save files and store plot images."""

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    headers = [
        "N",
        "Mode",
        "MyIndex_ms",
        "PostgreSQL_ts_rank_ms",
        "PostgreSQL_ts_rank_cd_ms",
        "ExactMatch",
        "Jaccard",
    ]
    if RUN_GIST:
        headers.append("PostgreSQL_ts_rank_GiST_ms")
    headers.extend(["IDs_sample", "MyIndex_sim", "Pg_sim", "JSON"])

    table_rows: list[list[str]] = []
    csv_rows: list[list[str]] = []

    for row in results:
        n, label = row[0], row[1]
        entry = entry_map[n]
        if label == "web":
            prefix = "web_"
        elif label == "custom":
            prefix = "custom_"
        else:
            prefix = "plain_"

        ids_short = ",".join(entry["MyIndex"]["ids"][0][:3])
        if len(entry["MyIndex"]["ids"][0]) > 3:
            ids_short += "..."
        mi_sim_short = ",".join(
            f"{s:.2f}" for s in entry["MyIndex"]["sims"][0][:3]
        )
        pg_sim_short = ",".join(
            f"{s:.2f}" for s in entry[f"{prefix}ts_rank"]["sims"][0][:3]
        )

        row_str = [
            str(n),
            label,
            f"{row[2]:.3f}",
            f"{row[3]:.3f}",
            f"{row[4]:.3f}",
            f"{row[5]:.3f}",
            f"{row[6]:.3f}",
        ]
        csv_row = row_str.copy()
        if RUN_GIST:
            if len(row) == 8:
                row_str.append(f"{row[7]:.3f}")
                csv_row.append(f"{row[7]:.3f}")
            else:
                row_str.append("")
                csv_row.append("")

        row_str.extend([ids_short, mi_sim_short, pg_sim_short, JSON_FILE])
        table_rows.append(row_str)

        ids_full = ",".join(entry["MyIndex"]["ids"][0][:5])
        mi_sim_full = ",".join(
            f"{s:.4f}" for s in entry["MyIndex"]["sims"][0][:5]
        )
        pg_sim_full = ",".join(
            f"{s:.4f}" for s in entry[f"{prefix}ts_rank"]["sims"][0][:5]
        )
        csv_row.extend([ids_full, mi_sim_full, pg_sim_full, JSON_FILE])
        csv_rows.append(csv_row)

    print_table(headers, table_rows)
    if save_files:
        write_csv("benchmark_results.csv", headers, csv_rows)
        save_full_results(full, JSON_FILE)

    import pandas as pd
    import matplotlib.pyplot as plt

    grouped: dict[str, dict[str, list[float]]] = {}
    for row in results:
        n, label = row[0], row[1]
        d = grouped.setdefault(label, {"N": [], "MyIndex": [], "ts_rank": [], "ts_rank_cd": [], "GiST": []})
        d["N"].append(n)
        d["MyIndex"].append(row[2])
        d["ts_rank"].append(row[3])
        d["ts_rank_cd"].append(row[4])
        if RUN_GIST and len(row) == 8:
            d["GiST"].append(row[7])

    for label, d in grouped.items():
        max_len = len(d["N"])
        for key in d:
            if len(d[key]) < max_len:
                d[key].extend([None] * (max_len - len(d[key])))
        df = pd.DataFrame(d)
        plt.figure(figsize=(10, 6))
        plt.plot(df["N"], df["MyIndex"], label="MyIndex", marker="o")
        plt.plot(df["N"], df["ts_rank"], label="ts_rank", marker="o")
        plt.plot(df["N"], df["ts_rank_cd"], label="ts_rank_cd", marker="o")
        if RUN_GIST and any(d["GiST"]):
            plt.plot(df["N"], d["GiST"], label="GiST", marker="o")
        plt.xlabel("Tamaño del Dataset (N)")
        plt.ylabel("Tiempo de búsqueda (ms)")
        plt.title(f"Comparación de Tiempos - {label}")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.xticks(df["N"], rotation=45)
        plt.savefig(out_path / f"time_{label}.png")
        plt.close()

    n_values = [entry["N"] for entry in full]
    for label in grouped.keys():
        if label == "web":
            prefix = "web_"
        elif label == "custom":
            prefix = "custom_"
        else:
            prefix = "plain_"
        for k in TOP_K_VALUES:
            key = str(k)
            vals = [entry["metrics"][f"{prefix}ts_rank"][key]["jacc_avg"] for entry in full]
            plt.figure(figsize=(10, 6))
            plt.plot(n_values, vals, marker="o")
            plt.xlabel("Tamaño del Dataset (N)")
            plt.ylabel("Jaccard")
            plt.title(f"Jaccard Promedio Top {k} - {label}")
            plt.grid(True)
            plt.tight_layout()
            plt.xticks(n_values, rotation=45)
            plt.savefig(out_path / f"jaccard_avg_{label}_top{k}.png")
            plt.close()

        num_q = len(ALL_QUERIES)
        for q_i in range(num_q):
            plt.figure(figsize=(10, 6))
            for k in TOP_K_VALUES:
                key = str(k)
                vals = [
                    entry["metrics"][f"{prefix}ts_rank"][key]["jacc_list"][q_i]
                    for entry in full
                ]
                plt.plot(n_values, vals, marker="o", label=f"Top {k}")
            plt.xlabel("Tamaño del Dataset (N)")
            plt.ylabel("Jaccard")
            plt.title(f"Query {q_i + 1} - {label}")
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            plt.xticks(n_values, rotation=45)
            plt.savefig(out_path / f"query{q_i + 1}_{label}.png")
            plt.close()


def main() -> None:
    """Run benchmarks for ``InvertedIndex`` and PostgreSQL."""
    results: list[list[float | int]] = []
    full: list[dict] = []

    if Path(JSON_FILE).exists() and Path("benchmark_results.csv").exists():
        print("Reusing existing benchmark data")
        with open(JSON_FILE, "r", encoding="utf-8") as f:
            full = json.load(f)
        with open("benchmark_results.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                r = [
                    int(row["N"]),
                    row["Mode"],
                    float(row["MyIndex_ms"]),
                    float(row["PostgreSQL_ts_rank_ms"]),
                    float(row["PostgreSQL_ts_rank_cd_ms"]),
                    float(row["ExactMatch"]),
                    float(row["Jaccard"]),
                ]
                gist_val = row.get("PostgreSQL_ts_rank_GiST_ms")
                if RUN_GIST and gist_val:
                    try:
                        r.append(float(gist_val))
                    except ValueError:
                        pass
                results.append(r)
        entry_map = {e["N"]: e for e in full}
        generate_outputs(results, full, entry_map, save_files=False, output_dir=PLOTS_DIR)
        return

    conn = None
    if RUN_POSTGRES:
        try:
            conn = psycopg2.connect(**PG_PARAMS)
            conn.autocommit = True
        except Exception as exc:
            print(f"PostgreSQL connection failed: {exc}")
            conn = None
    max_k = max(TOP_K_VALUES)
    for n in SIZES:
        print(f"Benchmarking MyIndex with N={n}")
        my_time, my_ids, my_sims = benchmark_myindex(n, ALL_QUERIES, max_k)

        entry: dict[str, object] = {
            "N": n,
            "MyIndex": {
                "time_ms": my_time,
                "ids": my_ids,
                "sims": my_sims,
            },
            "metrics": {},
        }

        for func in QUERY_FUNCS:
            if func == "websearch_to_tsquery":
                label = "web"
            elif func == "custom_bow":
                label = "custom"
            else:
                label = "plain"
            if conn:
                print(f"[{label}] Benchmarking ts_rank with N={n}")
                pg_time, pg_ids, pg_sims = benchmark_postgres(
                    n,
                    ALL_QUERIES,
                    conn,
                    ranking="ts_rank",
                    query_func=func,
                    top_k=max_k,
                )

                print(f"[{label}] Benchmarking ts_rank_cd with N={n}")
                pg_cd_time, pg_cd_ids, pg_cd_sims = benchmark_postgres(
                    n,
                    ALL_QUERIES,
                    conn,
                    ranking="ts_rank_cd",
                    query_func=func,
                    top_k=max_k,
                )
            else:
                pg_time, pg_ids, pg_sims = 0.0, [
                    [] for _ in ALL_QUERIES
                ], [
                    [] for _ in ALL_QUERIES
                ]
                pg_cd_time, pg_cd_ids, pg_cd_sims = 0.0, [
                    [] for _ in ALL_QUERIES
                ], [
                    [] for _ in ALL_QUERIES
                ]

            gist_time = None
            gist_ids: list[list[str]] | None = None
            gist_sims: list[list[float]] | None = None
            if conn and RUN_GIST and n in GIST_SIZES:
                print(
                    f"[{label}] Benchmarking PostgreSQL GiST ts_rank "
                    f"with N={n}"
                )
                gist_time, gist_ids, gist_sims = benchmark_postgres(
                    n,
                    ALL_QUERIES,
                    conn,
                    query_func=func,
                    use_gist=True,
                    top_k=max_k,
                )

            for q_i, q in enumerate(ALL_QUERIES):
                print(f"- Query '{q}' ({label}):")
                print(f"  MyIndex: {my_ids[q_i]}")
                print(f"  ts_rank: {pg_ids[q_i]}")
                print(f"  ts_rank_cd: {pg_cd_ids[q_i]}")
                if gist_ids:
                    print(f"  GiST: {gist_ids[q_i]}")

            metrics_ts = compute_quality(my_ids, pg_ids, TOP_K_VALUES)
            jacc = metrics_ts[TOP_K_VALUES[0]]["jacc_avg"]
            exact = metrics_ts[TOP_K_VALUES[0]]["exact_avg"]

            print(f"[{label}] Jaccard ts_rank vs MyIndex: {jacc:.3f}")

            row = [n, label, my_time, pg_time, pg_cd_time, exact, jacc]
            if gist_time is not None:
                row.append(gist_time)
            results.append(row)

            prefix = f"{label}_"
            entry[f"{prefix}ts_rank"] = {
                "time_ms": pg_time,
                "ids": pg_ids,
                "sims": pg_sims,
            }
            entry[f"{prefix}ts_rank_cd"] = {
                "time_ms": pg_cd_time,
                "ids": pg_cd_ids,
                "sims": pg_cd_sims,
            }
            m: dict[str, dict] = {
                f"{prefix}ts_rank": metrics_ts
            }
            metrics_cd = compute_quality(
                my_ids,
                pg_cd_ids,
                TOP_K_VALUES,
            )
            pgcd_jacc = metrics_cd[TOP_K_VALUES[0]]["jacc_avg"]
            print(f"[{label}] Jaccard ts_rank_cd vs MyIndex: {pgcd_jacc:.3f}")
            m[f"{prefix}ts_rank_cd"] = metrics_cd
            if (
                gist_time is not None
                and gist_ids is not None
                and gist_sims is not None
            ):
                entry[f"{prefix}GiST"] = {
                    "time_ms": gist_time,
                    "ids": gist_ids,
                    "sims": gist_sims,
                }
                metrics_g = compute_quality(
                    my_ids,
                    gist_ids,
                    TOP_K_VALUES,
                )
                g_jacc = metrics_g[TOP_K_VALUES[0]]["jacc_avg"]
                print(f"[{label}] Jaccard GiST vs MyIndex: {g_jacc:.3f}")
                m[f"{prefix}GiST"] = metrics_g
            entry["metrics"].update(m)

        full.append(entry)

    entry_map = {e["N"]: e for e in full}

    if conn:
        conn.close()

    generate_outputs(results, full, entry_map, output_dir=PLOTS_DIR)


if __name__ == '__main__':
    main()
