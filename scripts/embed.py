from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType, utility
from sentence_transformers import SentenceTransformer
import json

# 1. Koneksi ke Milvus
connections.connect("default", host="localhost", port="19530")

# 2. Load model embedding 
model = SentenceTransformer("intfloat/multilingual-e5-large", device='cpu')

# 3. Baca data dari file JSONL
documents = []
with open("chunked_token.jsonl", "r", encoding="utf-8") as f:
    for line in f:
        item = json.loads(line)
        documents.append(item)

print(f"Loaded {len(documents)} documents")

# 4. Siapkan field untuk dimasukkan ke Milvus
texts, file_names, page_numbers, bookmarks = [], [], [], []
text_lengths, has_tables_flags, chapter_titles = [], [], []

print("Processing documents...")

for doc in documents:
    texts.append(doc["text"])
    file_names.append(doc.get("file_name", ""))
    page_numbers.append(doc.get("page_number", 0))
    bookmarks.append(doc.get("bookmark", ""))
    text_lengths.append(doc.get("text_length", len(doc["text"])))
    has_tables_flags.append(doc.get("has_tables", False))
    chapter_titles.append(doc.get("chapter_title", ""))

# 5. Generate embeddings (GANTI di sini)
print("Generating embeddings...")
# bge-m3: gunakan mean pooling default dan JANGAN dinormalisasi
embeddings = model.encode(texts, normalize_embeddings=False).tolist()

# 6. Siapkan collection schema
collection_name = "pln_embeddings_simplified"
if collection_name in utility.list_collections():
    print(f"Dropping existing collection: {collection_name}")
    utility.drop_collection(collection_name)

dim = len(embeddings[0])
fields = [
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
    FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
    FieldSchema(name="file_name", dtype=DataType.VARCHAR, max_length=512),
    FieldSchema(name="page_number", dtype=DataType.INT64),
    FieldSchema(name="bookmark", dtype=DataType.VARCHAR, max_length=1024),
    FieldSchema(name="text_length", dtype=DataType.INT64),
    FieldSchema(name="has_tables", dtype=DataType.BOOL),
    FieldSchema(name="chapter_title", dtype=DataType.VARCHAR, max_length=1024),
]

schema = CollectionSchema(fields, description="Simplified PLN embedding with selected metadata")
collection = Collection(name=collection_name, schema=schema)

# 7. Buat index
print("Creating index...")
collection.create_index(field_name="embedding", index_params={
    "metric_type": "COSINE",
    "index_type": "IVF_FLAT",
    "params": {"nlist": 128}
})

# 8. Insert data
print("Inserting data to Milvus...")
insert_data = [
    texts,
    embeddings,
    file_names,
    page_numbers,
    bookmarks,
    text_lengths,
    has_tables_flags,
    chapter_titles
]
collection.insert(insert_data)
collection.load()

print(f"\n{len(texts)} dokumen berhasil di-insert ke Milvus.")

# ======================
# Fungsi Update Metadata
# ======================
def update_document_metadata_by_filename_page(filename: str, page_number: int, updated_fields: dict):
    print(f"\nUpdating document in '{filename}', page {page_number}")
    results = collection.query(
        expr=f'file_name == "{filename}" && page_number == {page_number}',
        output_fields=["id"]
    )
    if not results:
        print(" Dokumen tidak ditemukan.")
        return
    pk = results[0]["id"]
    collection.update(pk_field="id", data={"id": pk, **updated_fields})
    print("Metadata berhasil di-update.")

# ======================
# Fungsi Hapus Dokumen
# ======================
def delete_document_by_filename_page(filename: str, page_number: int):
    print(f"\nDeleting document in '{filename}', page {page_number}")
    collection.delete(expr=f'file_name == "{filename}" && page_number == {page_number}')
    print(" Dokumen berhasil dihapus.")

# ======================
# HYBRID SEARCH FUNCTION
# ======================
def hybrid_search(query_text, filter_expr="", top_k=5):
    print(f"\n Hybrid Search for: '{query_text}' with filter: {filter_expr}")
    query_embedding = model.encode([query_text], normalize_embeddings=False).tolist()

    search_params = {
        "metric_type": "COSINE",
        "params": {"nprobe": 10}
    }

    results = collection.search(
        data=query_embedding,
        anns_field="embedding",
        param=search_params,
        limit=top_k,
        output_fields=[
            "file_name", "page_number", "bookmark", "text", "text_length", "has_tables", "chapter_title"
        ],
        expr=filter_expr if filter_expr else None
    )

    for i, hit in enumerate(results[0]):
        print(f"\nResult {i+1}")
        print(f"Score: {hit.score:.4f}")
        print(f"File: {hit.entity['file_name']} - Page: {hit.entity['page_number']}")
        print(f"Chapter: {hit.entity['chapter_title']}")
        print(f"Text: {hit.entity['text'][:200]}...")
        print(f"Has Tables: {hit.entity['has_tables']}")
    return results

# ======================
# LOAD MANAGEMENT
# ======================
def load_selected_data(file_filter=None):
    collection.release()
    if file_filter:
        print(f" Loading subset where file_name == '{file_filter}'")
        expr = f'file_name == "{file_filter}"'
        collection.load(expr=expr)
    else:
        print(" Loading full collection...")
        collection.load()

# ======================
# UPDATE / UPSERT FUNCTION
# ======================
def upsert_document_by_filename_page(filename: str, page_number: int, updated_fields: dict):
    print(f"\n Upserting metadata for: {filename}, page {page_number}")
    results = collection.query(
        expr=f'file_name == "{filename}" && page_number == {page_number}',
        output_fields=["id"]
    )

    if results:
        pk = results[0]["id"]
        print("Found existing document. Updating...")
        collection.update(pk_field="id", data={"id": pk, **updated_fields})
        print("Metadata berhasil di-update.")
    else:
        print("Document not found. Inserting new entry...")
        dummy_text = updated_fields.get("text", "[NEW]")
        dummy_embedding = model.encode([dummy_text], normalize_embeddings=False).tolist()[0]

        insert_data = [
            [dummy_text],  # text
            [dummy_embedding],
            [filename],
            [page_number],
            [updated_fields.get("bookmark", "")],
            [updated_fields.get("text_length", len(dummy_text))],
            [updated_fields.get("has_tables", False)],
            [updated_fields.get("chapter_title", "")]
        ]
        collection.insert(insert_data)
        print(" Dokumen baru berhasil dimasukkan.")

# ======================
# SAFE DELETE FUNCTION
# ======================
def safe_delete_document_by_filename_page(filename: str, page_number: int):
    print(f"\n Attempting to delete: {filename}, page {page_number}")
    results = collection.query(
        expr=f'file_name == "{filename}" && page_number == {page_number}',
        output_fields=["id"]
    )
    if not results:
        print("Dokumen tidak ditemukan. Tidak ada yang dihapus.")
        return
    collection.delete(expr=f'file_name == "{filename}" && page_number == {page_number}')
    print("Dokumen berhasil dihapus.")

    