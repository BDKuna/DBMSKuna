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
from core.text_file import TextFile

# Reuse preprocessing files if they already exist
REUSE_INDEX = True

# Custom sizes can override the default benchmarking scale
CUSTOM_SIZES: list[int] | None = []
JSON_FILE = "benchmark_results_full.json"


# Dataset sizes used for each benchmark iteration
SIZES = CUSTOM_SIZES or [1000, 5000, 10000, 20000, 40000, 80000, 160000]

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
]

# Additional edge-case queries
ALL_QUERIES = QUERIES + [
    '"jurassic park"',
    "matrix",
    "jurassic park park",
    "jurassic & park",
    "jurassic | monster",
]

# Sizes used when running the optional GiST experiment
GIST_SIZES = [16000, 64000]
RUN_GIST = True

# Path to the full dataset
DATASET_PATH = Path("datasets/data2/mpst_full_data.csv")
# Temporary directory for generated CSV files
TMP_DIR = Path(".")

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
    if not Path(tf.data_path).exists():
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



def benchmark_myindex(n: int, queries: list[str]) -> tuple[float, list[list[str]]]:
    """Build ``InvertedIndex`` and return timing and results for each query."""
    tmp_csv = create_tmp_csv(n)
    index_path = cache_index(tmp_csv)
    idx = InvertedIndex(index_path)
    built_flag = Path(index_path + ".built")
    if not (REUSE_INDEX and built_flag.exists()):
        idx.buildIndex()
        built_flag.touch()

    timings: list[float] = []
    results: list[list[str]] = []
    for q in queries:
        res = None
        start = time.perf_counter()
        for _ in range(5):
            res = idx.searchQuery(q, limit=5)
        timings.append((time.perf_counter() - start) / 5)
        ids = [doc_id for doc_id, _ in res or []]
        results.append(ids)

    avg_ms = sum(timings) / len(timings) * 1000

    if not REUSE_INDEX:
        base = tmp_csv.name
        inv_dat = tmp_csv.parent / f"{base}_inv.dat"
        doc_dat = tmp_csv.parent / f"{base}_doc.dat"
        data_dat = tmp_csv.parent / f"{base}_data.dat"
        lengths_dat = tmp_csv.parent / f"{base}_lengths.dat"
        extra_lengths = tmp_csv.parent / (
            f"{base}_lengths.dat_lengths.dat"
        )

        for path in (
            tmp_csv,
            inv_dat,
            doc_dat,
            data_dat,
            lengths_dat,
            extra_lengths,
        ):
            if path.exists():
                path.unlink()

    return avg_ms, results


def parse_execution_time(plan_rows: list[tuple[str]]) -> float:
    """Extract execution time from ``EXPLAIN ANALYZE`` output."""
    text = "\n".join(r[0] for r in plan_rows)
    match = re.search(r"Execution Time: ([0-9.]+) ms", text)
    return float(match.group(1)) if match else 0.0


def tsquery_function(query: str) -> tuple[str, str]:
    """Select the PostgreSQL tsquery function for a given query."""
    if query.startswith('"') and query.endswith('"'):
        return "phraseto_tsquery", query.strip('"')
    if "&" in query or "|" in query:
        return "to_tsquery", query
    return "plainto_tsquery", query


def compute_quality(
    my_res: list[list[str]], pg_res: list[list[str]]
) -> tuple[float, float]:
    """Return exact-match ratio and average Jaccard index."""
    exact = [1.0 if set(m) == set(p) else 0.0 for m, p in zip(my_res, pg_res)]
    jacc = []
    for m, p in zip(my_res, pg_res):
        s1, s2 = set(m), set(p)
        inter = len(s1 & s2)
        union = len(s1 | s2)
        jacc.append(inter / union if union else 1.0)
    return sum(exact) / len(exact), sum(jacc) / len(jacc)


def benchmark_postgres(
    n: int,
    queries: list[str],
    conn,
    ranking: str = "ts_rank",
    use_gist: bool = False,
) -> tuple[float, list[list[str]]]:
    """Load data into PostgreSQL and run text search."""
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
    results: list[list[str]] = []
    for q in queries:
        func, prepared = tsquery_function(q)
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
                LIMIT 5
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
                LIMIT 5""",
            {"q": prepared},
        )
        ids = [str(row[0] - 1) for row in cur.fetchall()]
        results.append(ids)
    avg_ms = sum(timings) / len(timings)

    cur.close()
    if not REUSE_INDEX and tmp_csv.exists():
        os.remove(tmp_csv)

    return avg_ms, results


def save_full_results(data: list[dict], path: str) -> None:
    """Persist the extended benchmark results."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def main() -> None:
    """Run benchmarks for ``InvertedIndex`` and PostgreSQL."""
    results: list[list[float | int]] = []
    full: list[dict] = []

    try:
        conn = psycopg2.connect(**PG_PARAMS)
        conn.autocommit = True
    except Exception as exc:
        print(f"PostgreSQL connection failed: {exc}")
        conn = None
    for n in SIZES:
        print(f"Benchmarking MyIndex with N={n}")
        my_time, my_res = benchmark_myindex(n, ALL_QUERIES)

        if conn:
            print(f"Benchmarking PostgreSQL ts_rank with N={n}")
            pg_time, pg_res = benchmark_postgres(n, ALL_QUERIES, conn)

            print(f"Benchmarking PostgreSQL ts_rank_cd with N={n}")
            pg_cd_time, pg_cd_res = benchmark_postgres(
                n, ALL_QUERIES, conn, ranking="ts_rank_cd"
            )
        else:
            pg_time, pg_res = 0.0, [[] for _ in ALL_QUERIES]
            pg_cd_time, pg_cd_res = 0.0, [[] for _ in ALL_QUERIES]

        gist_time = None
        gist_res: list[list[str]] | None = None
        if conn and RUN_GIST and n in GIST_SIZES:
            print(f"Benchmarking PostgreSQL GiST ts_rank with N={n}")
            gist_time, gist_res = benchmark_postgres(
                n, ALL_QUERIES, conn, use_gist=True
            )

        for q_i, q in enumerate(ALL_QUERIES):
            print(f"- Query '{q}':")
            print(f"  MyIndex: {my_res[q_i]}")
            print(f"  ts_rank: {pg_res[q_i]}")
            print(f"  ts_rank_cd: {pg_cd_res[q_i]}")
            if gist_res:
                print(f"  GiST: {gist_res[q_i]}")

        exact, jacc = compute_quality(my_res, pg_res)

        row = [n, my_time, pg_time, pg_cd_time, exact, jacc]
        if gist_time is not None:
            row.append(gist_time)

        results.append(row)

        full.append(
            {
                "N": n,
                "MyIndex": {
                    "time_ms": my_time,
                    "results": my_res,
                },
                "ts_rank": {
                    "time_ms": pg_time,
                    "results": pg_res,
                },
                "ts_rank_cd": {
                    "time_ms": pg_cd_time,
                    "results": pg_cd_res,
                },
                "metrics": {
                    "exact_match": exact,
                    "jaccard": jacc,
                },
            }
        )
        if gist_time is not None and gist_res is not None:
            full[-1]["GiST"] = {
                "time_ms": gist_time,
                "results": gist_res,
            }

    if conn:
        conn.close()

    headers = [
        "N",
        "MyIndex_ms",
        "PostgreSQL_ts_rank_ms",
        "PostgreSQL_ts_rank_cd_ms",
        "ExactMatch",
        "Jaccard",
    ]
    if RUN_GIST:
        headers.append("PostgreSQL_ts_rank_GiST_ms")
    headers.extend(["IDs_sample", "JSON"])

    format_header = (
        f"| {'N':<6}| {'MyIndex_ms':>14} | {'PostgreSQL_ts_rank_ms':>24} | "
        f"{'PostgreSQL_ts_rank_cd_ms':>27} | {'ExactMatch':>10} | {'Jaccard':>8}"
    )
    if RUN_GIST:
        format_header += " | {:>27}".format("PostgreSQL_ts_rank_GiST_ms")
    format_header += " | {:>10} | {:>8} |"
    print(format_header)

    separator = (
        "|-----|--------------:|------------------------:|"
        "---------------------------:|-----------:|---------:"
    )  # noqa: E501
    if RUN_GIST:
        separator += "|-------------------------:|"
    separator += "|------------|------|"
    print(separator)

    for idx, row in enumerate(results):
        ids_snippet = ",".join(full[idx]["MyIndex"]["results"][0][:3])
        if len(full[idx]["MyIndex"]["results"][0]) > 3:
            ids_snippet += "..."
        line = (
            f"| {row[0]:<4}| {row[1]:14.3f} | {row[2]:24.3f} | {row[3]:27.3f} | "
            f"{row[4]:10.2f} | {row[5]:8.2f}"
        )
        if RUN_GIST and len(row) == 7:
            line += f" | {row[6]:27.3f}"
        line += f" | {ids_snippet:<10} | {JSON_FILE:<8} |"
        print(line)

    with open("benchmark_results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in results:
            writer.writerow([f"{v:.3f}" if isinstance(v, float) else v for v in row])

    save_full_results(full, JSON_FILE)


if __name__ == '__main__':
    main()
