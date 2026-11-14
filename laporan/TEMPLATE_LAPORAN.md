# Laporan Milestone 2a / Komparasi Kinerja PySpark
**Kelompok:** ...  **Anggota:** ...  **Run ID:** ...

## 1. Lingkungan pengujian
Perangkat (RAM/CPU), versi Docker, jumlah row input, partisi, jumlah bucket salt, AQE on/off, auto-broadcast threshold.

## 2. Topologi dan aliran data
Diagram Kafka topic -> consumer -> MinIO raw JSONL -> Spark -> Iceberg curated.

## 3. Validasi dan kualitas data
Tabel jumlah raw, valid, invalid/duplikat; alasan aturan cleaning; pemeriksaan Iceberg row count.

## 4. Analisis execution plan dan shuffle
Screenshot SQL dan Stages Spark UI; lokasi operator Exchange; shuffle read/write; durasi task.

## 5. Penanganan data skew
Distribusi partisi sebelum/sesudah salting; hasil metrik kota yang identik; trade-off overhead.

## 6. Broadcast join
Bukti SortMergeJoin vs BroadcastHashJoin dan durasi; batasan memori.

## 7. Partition pruning dan Iceberg
Kueri tanggal tertentu, bukti filter scan pada plan, snapshot terbaru, file curated.

## 8. Benchmark komparatif
| Eksperimen | Waktu percobaan 1 | Percobaan 2 | Percobaan 3 | Median | Temuan |
|---|---:|---:|---:|---:|---|
| GroupBy baseline | | | | | |
| Skew sebelum salting | | | | | |
| Skew setelah salting | | | | | |
| SortMergeJoin | | | | | |
| BroadcastHashJoin | | | | | |

## 9. Kesimpulan dan catatan keamanan
Jelaskan apakah optimasi berhasil; jika tidak, mengapa; bedakan local mode dan distributed cluster.

## Lampiran
Link repo Git; output quality.json; screenshot Spark UI dengan job ID; screenshot MinIO dan Iceberg.
