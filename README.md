
## 🔹 Struktur Folder

- airflow/          # Airflow DAGs, logs, config
- config/           # Docker-compose dan volume untuk Milvus
- data/             # Data mentah dan hasil proses
- scripts/          # Script Python untuk ETL dan Milvus
- sumber_data/      # Data sumber

**Folder `scripts/` berisi:**

- `ekstrak.py` → ekstraksi data dari pdf pln
- `cleansing.py` → pembersihan data 
- `chunking.py` → pemecahan data menjadi bagian lebih kecil  
- `generate_embedding.py` → membuat vektor embedding  
- `insert_to_milvus.py` → menyimpan data embedding ke Milvus  
- `milvus.py`, `milvus_utils.py` → utilitas untuk Milvus  
- `hybrid_search.py` → pencarian berbasis embedding  

---

## 🔹 Prasyarat

1. **Python 3.10+**  
2. **Virtual Environment** (menggunakan `venv`)  
3. **Docker & Docker Compose** (untuk menjalankan Milvus)  

---

## 🔹 Instalasi

Clone repository:

    git clone <https://github.com/dwmhr12/etl.git>
    cd pln
    
Install dependencies:

    pip install -r requirements.txt

---

## 🔹 Menjalankan Milvus

Masuk ke folder `config`:

    cd config

Jalankan docker-compose:

    docker-compose up -d

**Layanan Milvus yang berjalan:**

- `etcd` → koordinasi Milvus  
- `minio` → object storage  
- `standalone` → Milvus server  
- `attu` → Milvus UI di port 8000  

Cek status container:

    docker ps

---

## 🔹 Workflow Eksekusi Script

Urutan script Python yang direkomendasikan:

1. `ekstrak.py` → mengekstrak data dari sumber  
2. `cleansing.py` → membersihkan data  
3. `chunking.py` → memecah data menjadi chunk  
4. `generate_embedding.py` → membuat embedding  
5. `insert_to_milvus.py` → menyimpan embedding ke Milvus  

Contoh menjalankan script:

    python scripts/ekstrak.py
    python scripts/cleansing.py
    python scripts/chunking.py
    python scripts/generate_embedding.py
    python scripts/insert_to_milvus.py

---

## 🔹 Airflow

Airflow digunakan untuk orkestrasi pipeline. Folder `airflow/` berisi:

- `dags/` → DAG untuk workflow ETL  
- `logs/` → log eksekusi Airflow  
- `airflow.cfg` → konfigurasi Airflow  

Jalankan Airflow:

    cd airflow
    airflow db init
    airflow webserver --port 8080
    airflow scheduler

---

## 🔹 Catatan 
- Pastikan Milvus sudah berjalan sebelum menjalankan `insert_to_milvus.py`  
- Gunakan Airflow untuk menjadwalkan dan memonitor workflow otomatis
