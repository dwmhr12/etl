import json
import time  
from pathlib import Path
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType, utility

BASE_DIR = Path(__file__).resolve().parent.parent

def insert_to_milvus(
    input_path = BASE_DIR / "data/processed/Kepdir 0306 Kepdir 2023_v7/Kepdir 0306 Kepdir 2023_ekstrak_chunked_embedding.jsonl",
    collection_name="pln_embeddingsv7"
):
    start_time = time.time()  

    # 1. Load data dari file embedding
    with open(input_path, "r", encoding="utf-8") as f:
        documents = [json.loads(line) for line in f]

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
        print(f"Membuat collection baru: {collection_name}")
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

    # 4. Insert
    print("Inserting data...")
    collection.insert([
        texts,
        embeddings,
        file_names,
        page_numbers,
        bookmarks,
        text_lengths,
        has_tables_flags,
        chapter_titles
    ])
    collection.load()
    print(f"{len(texts)} dokumen berhasil di-insert ke Milvus.")

    end_time = time.time()
    elapsed_time = end_time - start_time
    mins, secs = divmod(elapsed_time, 60)
    print(f"[DONE] Insert completed in {int(mins)} minutes {secs:.2f} seconds.")

if __name__ == "__main__":
    insert_to_milvus()
