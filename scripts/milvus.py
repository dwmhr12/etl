import json
import time  
from pathlib import Path
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType, utility

BASE_DIR = Path(__file__).resolve().parent.parent

def insert_to_milvus(
    input_path = BASE_DIR / "data/processed/Kepdir 0306 Kepdir 2023_v7/Kepdir 0306 Kepdir 2023_ekstrak_chunked_embedding.jsonl",
    collection_name="pln_embeddingsv7",
    batch_size=500  
):
    start_time = time.time()

    # 1. Load data dari file embedding
    with open(input_path, "r", encoding="utf-8") as f:
        documents = [json.loads(line) for line in f]

    # Ambil data per kolom
    texts = [doc["text"] for doc in documents]
    embeddings = [doc["embedding"] for doc in documents]
    file_names = [doc.get("file_name", "") for doc in documents]
    page_numbers = [doc.get("page_number", 0) for doc in documents]
    bookmarks = [doc.get("bookmark", "") for doc in documents]
    text_lengths = [doc.get("text_length", len(doc["text"])) for doc in documents]
    has_tables_flags = [doc.get("has_tables", False) for doc in documents]
    chapter_titles = [doc.get("chapter_title", "") for doc in documents]

    dim = len(embeddings[0])

    # 2. Koneksi ke Milvus
    connections.connect("default", host="localhost", port="19530")

    # 3. Buat collection kalau belum ada
    if collection_name not in utility.list_collections():
        print(f"[INFO] Membuat collection baru: {collection_name}")
        schema = CollectionSchema([
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="file_name", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="page_number", dtype=DataType.INT64),
            FieldSchema(name="bookmark", dtype=DataType.VARCHAR, max_length=1024),
            FieldSchema(name="text_length", dtype=DataType.INT64),
            FieldSchema(name="has_tables", dtype=DataType.BOOL),
            FieldSchema(name="chapter_title", dtype=DataType.VARCHAR, max_length=1024),
        ], description="PLN Embeddings")

        collection = Collection(name=collection_name, schema=schema)
        collection.create_index(
            field_name="embedding",
            index_params={"metric_type": "COSINE", "index_type": "IVF_FLAT", "params": {"nlist": 128}}
        )
    else:
        collection = Collection(collection_name)

    # 4. Batching insert
    total_docs = len(texts)
    print(f"[INFO] Memulai insert {total_docs} dokumen dalam batch...")

    for i in range(0, total_docs, batch_size):
        end = i + batch_size
        batch_texts = texts[i:end]
        batch_embeddings = embeddings[i:end]
        batch_file_names = file_names[i:end]
        batch_page_numbers = page_numbers[i:end]
        batch_bookmarks = bookmarks[i:end]
        batch_text_lengths = text_lengths[i:end]
        batch_has_tables = has_tables_flags[i:end]
        batch_chapter_titles = chapter_titles[i:end]

        collection.insert([
            batch_texts,
            batch_embeddings,
            batch_file_names,
            batch_page_numbers,
            batch_bookmarks,
            batch_text_lengths,
            batch_has_tables,
            batch_chapter_titles
        ])

        print(f"[BATCH] Inserted {min(end, total_docs)} / {total_docs} items.")

    collection.load()
    print(f"[SUCCESS] Total {total_docs} dokumen berhasil di-insert ke Milvus.")

    end_time = time.time()
    mins, secs = divmod(end_time - start_time, 60)
    print(f"[DONE] Insert completed in {int(mins)} minutes {secs:.2f} seconds.")

if __name__ == "__main__":
    insert_to_milvus()
