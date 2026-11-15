"""Menghitung baris ditolak pada tahap cleaning, langsung dari data raw MinIO."""
import argparse
import json
import os
from collections import Counter
from datetime import datetime

import boto3

p = argparse.ArgumentParser()
p.add_argument("--run-id", default="kelas-mg7-a")
a = p.parse_args()

s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "labadmin"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "labpassword123"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
prefix = f"events/run_id={a.run_id}/"
keys = [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket="raw", Prefix=prefix)
        for o in page.get("Contents", [])]
assert keys, "Belum ada objek raw. Jalankan consumer dahulu."

rows = []
for k in keys:
    body = s3.get_object(Bucket="raw", Key=k)["Body"].read().decode("utf-8")
    rows.extend(json.loads(line) for line in body.splitlines() if line.strip())


def tanggal_valid(v):
    try:
        datetime.strptime(str(v), "%Y-%m-%dT%H:%M:%S")
        return True
    except ValueError:
        return False


def aturan_gagal(r):
    gagal = []
    if r.get("order_id") is None:
        gagal.append("L103 order_id null")
    if str(r.get("city") or "").strip() == "":
        gagal.append("L104 city kosong")
    if not tanggal_valid(r.get("event_time")):
        gagal.append("L105 tanggal gagal parse")
    amt = r.get("amount")
    if amt is None or amt < 0:
        gagal.append("L106 amount null atau negatif")
    qty = r.get("quantity")
    if qty is None or qty <= 0:
        gagal.append("L107 quantity <= 0")
    pid = r.get("product_id")
    if pid is None or not (1 <= pid <= 200):
        gagal.append("L108 product_id di luar 1-200")
    return gagal


per_aturan = Counter()
kombinasi = Counter()
valid = []
for r in rows:
    g = aturan_gagal(r)
    if g:
        for x in g:
            per_aturan[x] += 1
        kombinasi[" + ".join(g)] += 1
    else:
        valid.append(r)

kunci = Counter((r.get("run_id"), r.get("order_id")) for r in valid)
duplikat = sum(n - 1 for n in kunci.values() if n > 1)
clean = len(kunci)

print("TOTAL_BARIS_RAW", len(rows))
print("--- gagal per aturan (satu baris bisa gagal di lebih dari satu aturan) ---")
for nama, n in sorted(per_aturan.items()):
    print(" ", nama, n)
print("--- kombinasi aturan yang gagal per baris ---")
for nama, n in kombinasi.items():
    print(" ", nama, n)
print("UNIK_DITOLAK_ATURAN", sum(kombinasi.values()))
print("DUPLIKAT_DIBUANG", duplikat)
print("CLEAN_HITUNG", clean)
print("REJECTED_OR_DUPLICATE_HITUNG", len(rows) - clean)