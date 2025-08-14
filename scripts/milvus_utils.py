from pymilvus import Collection
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("intfloat/multilingual-e5-large", device="cpu")
collection = Collection("pln_embeddings_simplified")

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
