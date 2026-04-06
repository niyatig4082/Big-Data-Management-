from pyspark.sql import SparkSession
from pyspark import SparkContext
import math
import subprocess, sys

try:
    import psutil
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil", "-q"])

DATA_DIR       = "data"
OUTPUT_DIR     = "output"
DISTANCE       = 6.0
CELL_SIZE      = 6.0   

spark = SparkSession.builder \
    .appName("CS585_P3_Queries") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext



def parse_row(line):
    """
    Parse a CSV line into (id, x, y, name, age, email).
    Skips the header row.
    """
    parts = line.split(",")
    if parts[0] == "id":
        return None  
    try:
        return (int(parts[0]), float(parts[1]), float(parts[2]),
                parts[3], int(parts[4]), parts[5])
    except:
        return None  


def parse_row_with_handshake(line):
    """
    Parse a CSV line into (id, x, y, name, age, email, handshake).
    Skips the header row.
    """
    parts = line.split(",")
    if parts[0] == "id":
        return None
    try:
        return (int(parts[0]), float(parts[1]), float(parts[2]),
                parts[3], int(parts[4]), parts[5], parts[6].strip())
    except:
        return None


def euclidean_distance(x1, y1, x2, y2):
    """Compute Euclidean distance between two 2D points."""
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)


def get_cell(x, y, cell_size=CELL_SIZE):
    """Return the grid cell (col, row) for a given point."""
    return (int(x // cell_size), int(y // cell_size))


def get_neighboring_cells(cell):
    """Return all 9 cells: the cell itself + its 8 neighbors."""
    cx, cy = cell
    return [(cx + dx, cy + dy)
            for dx in [-1, 0, 1]
            for dy in [-1, 0, 1]]


print("Loading data...")

people_raw = sc.textFile(f"{DATA_DIR}/PEOPLE")
people_rdd = people_raw.map(parse_row).filter(lambda x: x is not None)

connected_raw = sc.textFile(f"{DATA_DIR}/CONNECTED")
connected_rdd = connected_raw.map(parse_row).filter(lambda x: x is not None)

people_hs_raw = sc.textFile(f"{DATA_DIR}/PEOPLE_WITH_HANDSHAKE_INFO")
people_hs_rdd = people_hs_raw.map(parse_row_with_handshake).filter(lambda x: x is not None)


print("\n── Query 1 ─────────────────────────────────")
print("Finding (pj, connect-i) pairs within 6 units...\n")

connected_local = connected_rdd.collect()

connected_grid = {}
for row in connected_local:
    cid, cx, cy = row[0], row[1], row[2]
    cell = get_cell(cx, cy)
    if cell not in connected_grid:
        connected_grid[cell] = []
    connected_grid[cell].append((cid, cx, cy))

connected_grid_bc = sc.broadcast(connected_grid)

def find_close_connected(person):
    """
    For a person in PEOPLE, check neighboring cells in the CONNECTED grid.
    Return list of (pj_id, connect_i_id) pairs within 6 units.
    """
    pid, px, py = person[0], person[1], person[2]
    grid = connected_grid_bc.value
    results = []
    for neighbor_cell in get_neighboring_cells(get_cell(px, py)):
        if neighbor_cell in grid:
            for (cid, cx, cy) in grid[neighbor_cell]:
                if euclidean_distance(px, py, cx, cy) <= DISTANCE:
                    if pid != cid:
                        results.append((pid, cid))
    return results

q1_result = people_rdd.flatMap(find_close_connected)

q1_result \
    .map(lambda x: f"{x[0]},{x[1]}") \
    .saveAsTextFile(f"{OUTPUT_DIR}/Q1_result")

q1_count = q1_result.count()
print(f"Q1 total pairs found: {q1_count:,}")
print("Q1 sample output (pj_id, connect_i_id):")
for pair in q1_result.take(10):
    print(f"  {pair}")


print("\n── Query 2 ─────────────────────────────────")
print("Finding distinct pj IDs that were close to any connected person...\n")

q2_result = q1_result \
    .map(lambda pair: pair[0]) \
    .distinct()

q2_result \
    .map(str) \
    .saveAsTextFile(f"{OUTPUT_DIR}/Q2_result")

q2_count = q2_result.count()
print(f"Q2 distinct pj IDs found: {q2_count:,}")
print("Q2 sample output (pj_id):")
for pid in q2_result.take(10):
    print(f"  {pid}")



print("\n── Query 3 ─────────────────────────────────")
print("Counting close contacts for each HANDSHAKE=yes person...\n")

connected_hs_rdd = people_hs_rdd.filter(lambda r: r[6] == "yes")
all_hs_rdd       = people_hs_rdd  

tagged_connected = connected_hs_rdd.map(
    lambda r: ("C", r[0], r[1], r[2])
)
tagged_all = all_hs_rdd.map(
    lambda r: ("P", r[0], r[1], r[2])
)

def emit_to_neighbors(tagged_record):
    tag, rid, rx, ry = tagged_record
    cell = get_cell(rx, ry)
    for neighbor_cell in get_neighboring_cells(cell):
        yield (neighbor_cell, (tag, rid, rx, ry))

keyed_connected = tagged_connected.flatMap(emit_to_neighbors)
keyed_all       = tagged_all.flatMap(emit_to_neighbors)

combined = keyed_connected.union(keyed_all)
grouped  = combined.groupByKey()  

def count_close_contacts(cell_and_records):
    """
    Within a cell group, for each connected person (tag="C"),
    count how many people (tag="P") are within 6 units.
    Note: we exclude the connected person from their own count (same id).
    """
    _, records = cell_and_records
    records = list(records)

    connected_in_cell = [(rid, rx, ry) for (tag, rid, rx, ry) in records if tag == "C"]
    all_in_cell       = [(rid, rx, ry) for (tag, rid, rx, ry) in records if tag == "P"]

    results = []
    for (cid, cx, cy) in connected_in_cell:
        count = sum(
            1 for (pid, px, py) in all_in_cell
            if pid != cid and euclidean_distance(cx, cy, px, py) <= DISTANCE
        )
        results.append((cid, count))
    return results

q3_result = grouped \
    .flatMap(count_close_contacts) \
    .reduceByKey(lambda a, b: a + b)

q3_result \
    .map(lambda x: f"{x[0]},{x[1]}") \
    .saveAsTextFile(f"{OUTPUT_DIR}/Q3_result")

q3_count = q3_result.count()
print(f"Q3 connected people processed: {q3_count:,}")
print("Q3 sample output (connect_i_id, close_contact_count):")
for row in q3_result.take(10):
    print(f"  connect_id={row[0]}, close_contacts={row[1]}")

print("\nAll queries complete! Results saved to output/")
spark.stop()