from pymilvus import connections, Collection
from sentence_transformers import SentenceTransformer

#1 koneksi ke milvus
connections.connect("default", host="localhost", port="19530")

#2 load model embedding
#model = SentenceTransformer("intfloat/multilingual-e5-larga", device='cpu') #hanya jika pakai vektor search

#3 load collection
collection_name = "pln_embeddings_simplified"
collection = Collection(collection_name)
collection.load()

#4 metadata filtering only (tanpa vector)
filter_expr = 'chapter_title like "%Ketentuan Teknis%" && file_name == "DOKUMEN JUKNIS PLN SUCCESS PROFILE-V03.pdf"'

#query hanya berdasarkan metadata
result = collection.query(
    expr=filter_expr,
    output_fields=["text", "chapter_title", "file_name", "page_number"]
)

# Tampilkan hasil query
print(f"\nDitemukan {len(result)} hasil berdasarkan metadata filter:")
print("=" * 100)
for i, res in enumerate(result):
    print(f"[{i+1}] File Name   : {res['file_name']}")
    print(f"     Halaman     : {res['page_number']}")
    print(f"     Chapter     : {res['chapter_title']}")
    print(f"     Teks        : {res['text'][:200]}...")
    print("-" * 100)