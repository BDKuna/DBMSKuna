# core/stats.py

memory_accesses = {
    "reads": 0,
    "writes": 0
}

def reset_counters():
    memory_accesses["reads"] = 0
    memory_accesses["writes"] = 0

def count_read():
    memory_accesses["reads"] += 1

def count_write():
    memory_accesses["writes"] += 1

def get_counts():
    return dict(memory_accesses)  # Copia para no exponer la original
