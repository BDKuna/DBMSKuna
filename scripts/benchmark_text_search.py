"""Benchmarking text search using custom inverted index and PostgreSQL."""

import csv
import os
import re
import time
from pathlib import Path

import psycopg2

from indexes.invertedindex import InvertedIndex
from preprocessing.text import processingDatasetOnInvertedFile


# Dataset sizes used for each benchmark iteration
SIZES = [1000, 2000, 4000, 8000, 16000, 32000, 64000]

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
    """Create a temporary CSV with the first ``n`` rows of the dataset."""
    tmp_csv = TMP_DIR / f"tmp_mpst_{n}.csv"
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


def benchmark_myindex(n: int, queries: list[str]) -> float:
    """Build and query the custom inverted index."""
    tmp_csv = create_tmp_csv(n)
    index_path = processingDatasetOnInvertedFile(
        str(tmp_csv), column="plot_synopsis"
    )
    idx = InvertedIndex(index_path)
    idx.buildIndex()

    timings = []
    for q in queries:
        # Measure average search time over five executions
        start = time.perf_counter()
        for _ in range(5):
            idx.searchQuery(q, limit=5)
        timings.append((time.perf_counter() - start) / 5)

    avg_ms = sum(timings) / len(timings) * 1000

    base = tmp_csv.stem  # sin la extensión .csv
    inv_dat = tmp_csv.parent / f"{base}_inv.dat"
    doc_dat = tmp_csv.parent / f"{base}_doc.dat"

    for path in (tmp_csv, inv_dat, doc_dat):
        if path.exists():
            path.unlink()

    return avg_ms


def parse_execution_time(plan_rows: list[tuple[str]]) -> float:
    """Extract execution time from ``EXPLAIN ANALYZE`` output."""
    text = "\n".join(r[0] for r in plan_rows)
    match = re.search(r"Execution Time: ([0-9.]+) ms", text)
    return float(match.group(1)) if match else 0.0


def benchmark_postgres(n: int, queries: list[str], conn) -> float:
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
    # Create GIN index for full-text search
    cur.execute(
        "CREATE INDEX idx_movies_fts ON movies USING GIN(document_with_weights)"
    )
    conn.commit()

    timings = []
    for q in queries:
        total = 0.0
        for _ in range(5):
            cur.execute(
                """EXPLAIN (ANALYZE, BUFFERS)
                SELECT id,
                       ts_rank(document_with_weights,
                               plainto_tsquery('english', %(q)s)) AS rank
                FROM movies
                WHERE document_with_weights @@ plainto_tsquery('english', %(q)s)
                ORDER BY rank DESC
                LIMIT 5
                """,
                {"q": q},
            )
            plan = cur.fetchall()
            total += parse_execution_time(plan)
        timings.append(total / 5)
    avg_ms = sum(timings) / len(timings)

    cur.close()
    if tmp_csv.exists():
        os.remove(tmp_csv)

    return avg_ms


def main() -> None:
    """Run benchmarks for the custom index and PostgreSQL."""
    results_my: list[tuple[int, str, float]] = []
    results_pg: list[tuple[int, str, float]] = []

    conn = psycopg2.connect(**PG_PARAMS)
    conn.autocommit = True
    for n in SIZES:
        print(f"Benchmarking MyIndex with N={n}")
        my_time = benchmark_myindex(n, QUERIES)
        results_my.append((n, "MyIndex", my_time))

        print(f"Benchmarking PostgreSQL with N={n}")
        pg_time = benchmark_postgres(n, QUERIES, conn)
        results_pg.append((n, "PostgreSQL", pg_time))

    conn.close()

    header = f"| {'N':<6}| {'MyIndex (ms)':>12} | {'PostgreSQL (ms)':>15} |"
    print(header)
    print("|-----|--------------:|---------------:|")
    for (n, _, m), (_, __, p) in zip(results_my, results_pg):
        print(f"| {n:<4}| {m:12.3f} | {p:15.3f} |")

    with open("benchmark_results.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["N", "MyIndex_ms", "PostgreSQL_ms"])
        for (n, _, m), (_, __, p) in zip(results_my, results_pg):
            writer.writerow([n, f"{m:.3f}", f"{p:.3f}"])


if __name__ == '__main__':
    main()
