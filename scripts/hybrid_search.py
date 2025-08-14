from pymilvus import connections, Collection
from sentence_transformers import SentenceTransformer

# 1. Koneksi ke Milvus
connections.connect("default", host="localhost", port="19530")

# 2. Load model embedding
model = SentenceTransformer("intfloat/multilingual-e5-large", device='cpu')

# 3. Load collection
collection = Collection("pln_embeddings_simplified")
collection.load()

# 4. Pertanyaan & filter (gabungan vector + keyword)
question = "Final Capacity Index"
filter_keyword = "Ketentuan Teknis"  # ← ini bisa diganti sesuai kebutuhan
threshold = 0.80

# 5. Generate embedding dari pertanyaan
question_embedding = model.encode([question], normalize_embeddings=False).tolist()

# 6. Siapkan parameter pencarian
search_params = {
    "metric_type": "COSINE",
    "params": {"nprobe": 10}
}

# 7. Buat filter expression (contoh: chapter_title mengandung kata kunci tertentu)
filter_expr = f'chapter_title like "%{filter_keyword}%"'

# 8. Jalankan hybrid search (vector + filter expr)
results = collection.search(
    data=question_embedding,
    anns_field="embedding",
    param=search_params,
    limit=10,
    output_fields=["text", "bookmark", "file_name", "chapter_title"],
    expr=filter_expr
)

# 9. Tampilkan hasil berdasarkan threshold
print(f"\nPencarian: '{question}' + filter: '{filter_keyword}' (Threshold: {threshold*100}%)")
print("=" * 100)

found_relevant = False
for i, hit in enumerate(results[0]):
    if hit.score >= threshold:
        found_relevant = True
        print(f"[{i+1}] Skor: {hit.score:.4f}")
        print(f"     File Name     : {hit.entity.get('file_name', 'N/A')}")
        print(f"     Bookmark      : {hit.entity.get('bookmark', 'N/A')}")
        print(f"     Chapter Title : {hit.entity.get('chapter_title', 'N/A')}")
        print(f"     Teks          : {hit.entity.get('text', '')[:200]}...")
        print("-" * 100)

if not found_relevant:
    print(f"Tidak ada dokumen dengan skor >= {threshold*100}%")
    if results[0]:
        best_hit = results[0][0]
        print("\nHasil terbaik yang tersedia:")
        print(f" Skor            : {best_hit.score:.4f}")
        print(f" File Name       : {best_hit.entity.get('file_name', 'N/A')}")
        print(f" Bookmark        : {best_hit.entity.get('bookmark', 'N/A')}")
        print(f" Chapter Title   : {best_hit.entity.get('chapter_title', 'N/A')}")
        print(f" Teks            : {best_hit.entity.get('text', '')[:200]}...")
