# Panduan Membaca Kode per Sintaks — Praktikum Terintegrasi MG5–MG7

Gunakan sebagai pendamping **Modul Praktikum Minggu ke-7**. Saran belajar: (1) buka kode dalam editor, (2) temukan ekspresi pada tabel ini, (3) terangkan INPUT → PROSES → OUTPUT, (4) baru jalankan perintah. `run-id` semua tahap **harus sama**.

## 1. Peta lintasan dan tanggung jawab komponen

| Komponen | Apa yang dilakukan | Apa yang **tidak** dilakukan |
|---|---|---|
| Kafka-compatible Redpanda | Menyimpan event sebagai log berpartisi; producer menulis, consumer membaca. | Bukan object storage data lake. |
| Python consumer + boto3 | Membaca event dan mengemasnya ke objek JSONL pada MinIO. | Bukan Spark Structured Streaming. |
| MinIO raw | Menampung objek data mentah untuk pembacaan batch. | Tidak otomatis membersihkan atau mengindeks data. |
| Spark PySpark | Mendistribusikan operasi ETL, agregasi dan join dalam task/stage. | Tidak selalu paralel lebih cepat untuk data kecil. |
| Iceberg REST Catalog | Mengelola namespace dan metadata tabel; data tabel disimpan dalam warehouse bucket MinIO. | Endpoint catalog bukan endpoint objek S3. |
| Spark Web UI | Menampilkan Jobs, SQL, Stages, Executors dan metrik dari eksekusi yang benar-benar terjadi. | Tidak memberikan nilai performa “ideal” universal. |

**Empat konsep yang kerap tertukar:** `Kafka partition` = potongan log pesan broker; `S3 object key` = nama objek di MinIO, sering berisi string `partition=`; `Spark partition` = satu unit data yang diproses task; `Iceberg partition` = cara penyusunan file tabel untuk pemangkasan pemindaian. Keempatnya **tidak otomatis identik**.

## 2. `docker-compose.yml`: memahami istilah sebelum menjalankan

| Fragmen | Makna | Konsekuensi praktikum |
|---|---|---|
| `services:` | Definisi container dalam satu proyek. | `docker compose ps` menunjukkan nama service. |
| `image:` | Image yang ditarik dari registry. | Butuh internet pada pull pertama. |
| `build:` | Bangun image sendiri memakai Dockerfile. | Dependency Python/Java terinstal di image lab. |
| `ports: '9001:9001'` | HOST:CONTAINER. | Browser host menggunakan `localhost:9001`. |
| `environment:` | Variabel env untuk container. | Python membaca env via `os.getenv`. |
| `depends_on:` | Urutan start container. | **Bukan jaminan** service siap; periksa log. |
| `volumes:` | Data persisten atau bind mount. | Data MinIO tidak hilang hanya karena container berhenti. |
| `networks: [labnet]` | DNS antar-service. | Dari container gunakan `minio:9000`, bukan `localhost`. |
| `command: sleep infinity` | Mempertahankan container tools/spark hidup. | Perintah lab dijalankan memakai `docker compose exec ...`. |

## 3. `00_setup.py`: membuat bucket

- `import boto3`: akses SDK penyimpanan kompatibel S3.
- `os.getenv("S3_ENDPOINT", default)`: ambil alamat endpoint yang sesuai jaringan Docker; bila env tidak ada, pakai default.
- `boto3.client("s3", ...)`: buat objek koneksi API, **belum** membaca atau menyimpan record.
- `s3.create_bucket(Bucket="raw")`: siapkan bucket; sifatnya operasi administratif, bukan membuat folder `events`.
- `except ClientError`: tangani error bila bucket sudah dibuat; error koneksi/kredensial tetap perlu diperbaiki.

## 4. `01_producer.py`: menulis event (MG5)

| Sintaks | Input → Output | Alasan memilihnya |
|---|---|---|
| `argparse.ArgumentParser()` | Argument CLI → objek `args`. | Satu skrip bisa dipakai banyak eksperimen. |
| `--rows 20000` | Integer → jumlah event. | Menentukan skala beban. |
| `--run-id smoke-a` | String → label event. | Memisahkan dataset percobaan. |
| `KafkaProducer(bootstrap_servers=...)` | Broker address → koneksi producer. | Producer berjalan dari container tools. |
| `key_serializer=lambda v: v.encode("utf-8")` | String → bytes. | Kafka menyimpan key sebagai byte sequence. |
| `value_serializer=lambda v: json.dumps(v).encode("utf-8")` | Dictionary → UTF-8 JSON bytes. | Bahasa yang berbeda dapat membaca event. |
| `acks="all"` | Menunggu ACK replika in-sync. | Di satu broker RF=1, belum berarti tahan failover. |
| `retries=5` | Error retriable → kirim ulang. | Bukan otomatis exactly-once. |
| `i % 10 < 8` | Sisa bagi 10 → 80% satu kota. | Secara sengaja menciptakan ketidakseimbangan key. |
| `i % 997 == 0` | Tiap kelipatan 997 → kota kosong. | Menyediakan kasus quality filter. |
| `i % 1009 == 0` | Tiap kelipatan 1009 → amount NULL. | Mendorong pengecekan missing value. |
| `producer.send(...)` | Event → antrean producer asinkron. | Return dari `send` tidak identik pesan sudah tersimpan. |
| `producer.flush()` | Pending messages → konfirmasi penyelesaian. | Dipanggil sebelum exit. |
| `time.perf_counter()` | Jam monotonic resolusi tinggi → durasi. | Cocok untuk pengukuran elapsed time. |

**Perhatian:** Karena key Kafka adalah `product_id`, sebaran Kafka partition bergantung hash key produk; **skew Spark yang dipelajari nanti** muncul ketika Spark melakukan `repartition(..., "city")`, bukan otomatis mengikuti partisi Kafka.

## 5. `02_consumer_raw_minio.py`: consumer + commit offset (MG5 menuju MG6)

| Sintaks | Fungsi | Apa yang harus dicek |
|---|---|---|
| `KafkaConsumer(topic, group_id=...)` | Membaca topik dalam consumer group. | Nama topik tepat `ecommerce.orders`. |
| `enable_auto_commit=False` | Tidak commit offset secara berkala otomatis. | Commit hanya setelah simpan batch berhasil. |
| `auto_offset_reset="earliest"` | Grup baru membaca dari offset paling awal yang tersedia. | Tidak memaksa reset grup yang sudah memiliki offset. |
| `consumer.poll(timeout_ms=2000)` | Ambil sejumlah record per partition. | Jika kosong terlalu lama, periksa producer dan run-id. |
| `m.value.get("run_id")` | Batasi event milik eksperimen saat ini. | Input run-id producer dan consumer harus sama. |
| `json.dumps(record) + "\n"` | Dictionary → JSON per baris. | Banyak event menjadi satu objek `.jsonl`. |
| `put_object(Bucket="raw",Key=key,Body=...)` | Bytes JSONL → objek MinIO. | Di console muncul `events/run_id=...`. |
| `OffsetAndMetadata(next_offset, "")` | Offset berikutnya + metadata. | Commit setelah upload berhasil. |
| `consumer.commit(...)` | Menyimpan posisi konsumsi group. | Potensi replay tetap ada bila crash sebelum commit. |

**At-least-once:** penulisan sukses tetapi commit gagal dapat menyebabkan record dibaca ulang; proses Spark menghapus duplikasi berdasarkan `(run_id,order_id)`. Pola contoh ini cocok untuk kelas, tetapi produksi memerlukan strategi idempotensi dan fault recovery yang lebih kuat.

## 6. `04_spark_pipeline.py`: transformasi MG6

| Sintaks | Arti dan alasan |
|---|---|
| `SparkSession.builder.getOrCreate()` | Membuat/memperoleh sesi Spark; koneksi utama ke Spark SQL dan DataFrame. |
| `.config("spark.sql.shuffle.partitions", "8")` | Menentukan jumlah partisi shuffle sebagai baseline pembelajaran. |
| `.config("spark.sql.adaptive.enabled", "false")` | Mematikan AQE agar rencana query manual lebih mudah dibandingkan; produksi sering membiarkannya aktif. |
| `.config("spark.sql.autoBroadcastJoinThreshold", "-1")` | Menonaktifkan broadcast otomatis; eksperimen broadcast eksplisit tetap dapat memakai hint. |
| `StructType([StructField(...)])` | Skema eksplisit; mengurangi inference dari berkas raw. |
| `spark.read.schema(schema).json(path)` | Membaca JSONL menjadi DataFrame lazy. |
| `.withColumn("city", initcap(trim(...)))` | Normalisasi string tanpa mengubah raw MinIO. |
| `.withColumn("sale_date", to_date(...))` | Konversi event_time → kolom tanggal untuk kurasi/partisi. |
| `.filter(...)` | Keluarkan nilai kosong, null, angka tidak valid, dan tanggal gagal parse. |
| `.dropDuplicates(["run_id", "order_id"])` | Cegah satu transaksi dihitung dua kali. |
| `.persist(MEMORY_AND_DISK)` | Cache intermediate DataFrame untuk eksperimen berulang. |
| `cleaned.count()` | Action yang mengeksekusi DAG sekaligus materialisasi cache. |
| `sc.setJobGroup(tag,...)` | Label kelompok Spark job sehingga bukti UI bisa dihubungkan dengan eksperimen. |
| `.explain(mode="formatted")` | Menampilkan operator physical plan termasuk `Exchange` dan join. |
| `query.collect()` | Menjalankan query dan membawa hasil kecil ke driver; **jangan collect jutaan row**. |

## 7. `04_spark_pipeline.py`: DAG, shuffle, skew, salting (MG7)

```python
skewed = cleaned.repartition(8, "city")
```

`repartition(8,"city")` mendistribusikan record sesuai hash dari **city** ke delapan target partition. Karena hanya ada tiga kota (dan Denpasar sangat dominan), distribusi baris antartask dapat timpang. Baris tidak hilang; nilai yang sama cenderung masuk partition yang sama. Operasi ini biasanya memicu **shuffle**.

```python
salted = cleaned.withColumn(
    "salt", F.pmod(F.xxhash64("order_id"), F.lit(16))
).repartition(8, "city", "salt")
```

- `xxhash64(order_id)` menghasilkan hash 64-bit deterministik dari ID transaksi, sehingga salting konsisten tanpa random sampling.
- `pmod(hash,16)` memetakan hasil (termasuk hash negatif) ke angka 0..15; tiap nilai city memperoleh beberapa sub-key.
- `repartition(8,"city","salt")` memungkinkan sub-key kota panas tersebar lebih merata dalam delapan partisi.
- Salt 16 **bukan** otomatis 16 Spark partitions: jumlah Spark partitions di sini tetap 8; jumlah salt bucket mengatur variasi key.

```python
partial = salted.groupBy("city", "salt").agg(
    F.count("*").alias("orders_partial"),
    F.sum("amount").alias("sales_partial"))
final = partial.groupBy("city").agg(
    F.sum("orders_partial").alias("orders"),
    F.sum("sales_partial").alias("gross_sales"))
```

**Mengapa dua tahap?** Tahap pertama menghitung agregasi tiap pasangan `(city,salt)` agar satu hot key dapat dibagi. Tahap kedua menggabungkan hasil parsial kembali ke tingkat kota. Jika hanya menggunakan `city,salt` tanpa tahap kedua, output berisi banyak baris per kota dan **tidak setara** agregasi awal. Skrip memakai `assert result_a == result_b == result_c` untuk memastikan kebenaran.

`partition_sizes(df)` memanggil `df.rdd.mapPartitionsWithIndex(...)`: fungsi memberi indeks partition lalu menghitung jumlah row yang diproses, kemudian hanya angka ringkasnya dibawa ke driver. Ini operasi audit pembelajaran; tidak digunakan sebagai teknik optimasi produksi.

## 8. `04_spark_pipeline.py`: join dan Iceberg (MG7)

```python
merge_join = cleaned.join(dim.hint("merge"), "product_id", "inner")
broadcast_join = cleaned.join(F.broadcast(dim), "product_id", "inner")
```

- `dim` adalah tabel kategori produk kecil, dibuat dari `spark.range(1,201)`; bertipe lookup dimension.
- `hint("merge")` mendorong Spark memakai SortMergeJoin untuk eksperimen baseline. Spark masih dapat menolak hint pada kondisi tertentu; periksa physical plan.
- `F.broadcast(dim)` memberikan hint agar tabel kecil dikirim ke sisi pelaksana sehingga tabel besar tidak perlu shuffle untuk **join** itu. Operator yang diharapkan `BroadcastHashJoin` dan `BroadcastExchange`.
- `groupBy("category")` setelah join dapat tetap membuat shuffle agregasi. Jadi broadcast join tidak berarti query sama sekali bebas shuffle.
- `assert result_d == result_e` memeriksa hasil agregasi identik sebelum/sesudah perubahan join.

```python
cleaned.writeTo("lake.curated.orders_clean").using("iceberg") \
    .partitionedBy("sale_date").createOrReplace()
```

`lake` adalah nama Spark Catalog yang dikonfigurasi ke Iceberg REST; `curated` adalah namespace; `orders_clean` adalah tabel. `writeTo` memakai DataFrameWriterV2. `partitionedBy(sale_date)` menyimpan partisi Iceberg berdasarkan tanggal. `createOrReplace()` cocok untuk eksperimen kelas yang hendak mengulang hasil—**jangan gunakan tanpa memahami konsekuensi penggantian tabel pada data produksi**.

```python
spark.sql("SELECT * FROM lake.curated.orders_clean.snapshots").show(5, truncate=False)
```

Tabel metadata Iceberg `.snapshots` menyajikan snapshot tabel; ini bukan salinan seluruh baris transaksi. `truncate=False` agar string panjang tidak dipotong dalam hasil terminal.

```python
spark.table("lake.curated.orders_clean").filter(
    F.col("sale_date") == F.lit("2026-09-05").cast("date"))
```

Kondisi filter pada field partisi memungkinkan **partition pruning**: Spark/Iceberg berpeluang menghindari pembacaan berkas tanggal lain. Buktikan melalui operator Scan/Filters pada `plan_partition_pruning.txt`, bukan hanya dengan menghitung output query.

## 9. Membaca Spark Web UI tanpa salah interpretasi

1. **Jobs**: identifikasi pekerjaan yang dipicu oleh action; catat jumlah stage dan total durasi.
2. **Stages**: catat task paling lambat, median task, shuffle read/write, dan spilled bytes bila terlihat. Ketidakseimbangan task dapat menandakan skew—tetapi juga dapat disebabkan sumber daya lain.
3. **SQL**: buka physical plan query yang sesuai tag pekerjaan. Temukan `Exchange`, `SortMergeJoin`, `BroadcastHashJoin`, `HashAggregate`, dan `Scan`.
4. **Executors**: periksa storage dan aktivitas executor (pada `local[4]` semua berada dalam host yang sama).
5. Bedakan **transformasi yang direncanakan** (`DataFrame`) dengan **rencana aktual** yang dieksekusi; gunakan berkas `plan_*.txt` sebagai jejak.

**Perbandingan hasil:** Waktu lebih singkat tidak cukup. Harus ada (a) data yang sama, (b) hasil numerik identik, (c) perbedaan plan yang dibuktikan, dan (d) catatan kondisi pengukuran. Untuk laporan gunakan minimal tiga kali run dengan run-id/seed dan ukuran yang konsisten; jelaskan efek warm-up/cache.

## 10. Pemeriksaan otomatis akhir

Sesudah Spark selesai dan menyimpan berkas hasil:

```bash
docker compose exec tools python scripts/05_audit_hasil.py --run-id smoke-a
```

Yang diuji: kelengkapan berkas, `raw_count = clean_count + rejected`, keberadaan lima benchmark, total row semua partisi, serta operator `SortMergeJoin` vs `BroadcastHashJoin`. Ini **tidak** memeriksa apakah screenshot benar atau sistem benar-benar terdistribusi. Bukti UI dan narasi analitis wajib diperiksa secara manual.

## 11. Jika praktikum berhenti, baca kegagalan dari hulu ke hilir

1. Tidak ada pesan Kafka → cek broker + topic + producer.
2. Ada pesan, tidak ada objek raw → cek consumer `--run-id`, timeout, koneksi boto3 dan offset.
3. Ada objek raw, Spark gagal baca → cek path `s3a://raw/...`, bucket dan JAR Hadoop AWS.
4. Spark bisa cleaning, gagal Iceberg → cek REST endpoint, kredensial dan warehouse bucket.
5. Pipeline sukses, benchmark aneh → cek AQE, broadcast threshold, jumlah partisi, warm-up, ukuran dataset, dan level cache.

**Keterbatasan yang perlu disebutkan saat laporan:** default satu broker (RF=1) bukan bukti failover multi-broker; `local[4]` bukan cluster beberapa node; dataset sintetis tidak menggantikan data riil; throughput yang terukur tidak otomatis mencapai target RPS MG5.
