from pymilvus import connections, Collection
from sentence_transformers import SentenceTransformer

# 1. Koneksi ke Milvus
connections.connect("default", host="localhost", port="19530")

# 2. Load model
model = SentenceTransformer("intfloat/multilingual-e5-large", device='cpu')

# 3. Load collection
collection = Collection("pln_embeddings_simplified")
collection.load()

# 4. Pertanyaan
question = "Direktori Pembelajaran adalah"

# 5. Embeddingkan pertanyaan
question_embedding = model.encode([question]).tolist()

# 6. Lakukan search di Milvus
search_params = {
    "metric_type": "COSINE",
    "params": {"nprobe": 10}
}
results = collection.search(
    data=question_embedding,
    anns_field="embedding",
    param=search_params,
    limit=10,
    output_fields=["text", "bookmark", "file_name", "chapter_title"]
)

# 7. Filter dan cetak hasil dengan threshold 80%
THRESHOLD = 0.80

print(f"Pencarian: '{question}' (Threshold: {THRESHOLD*100}%)")
print("=" * 100)

found_relevant = False
for i, hit in enumerate(results[0]):
    if hit.distance >= THRESHOLD:
        found_relevant = True
        bookmark = hit.entity.get('bookmark', 'N/A')
        text = hit.entity.get('text', '')
        file_name = hit.entity.get('file_name', 'N/A')
        chapter_title = hit.entity.get('chapter_title', 'N/A')

        print(f"[{i+1}] Skor: {hit.distance:.4f}")
        print(f"     Bookmark       : {bookmark}")
        print(f"     File Name      : {file_name}")
        print(f"     Chapter Title  : {chapter_title}")
        print(f"     Teks           : {text}")
        print("-" * 100)

if not found_relevant:
    print(f"Tidak ada dokumen dengan skor >= {THRESHOLD*100}%")
    print("\nHasil terbaik yang tersedia:")
    if results[0]:
        best_hit = results[0][0]
        bookmark = best_hit.entity.get('bookmark', 'N/A')
        text = best_hit.entity.get('text', '')
        file_name = best_hit.entity.get('file_name', 'N/A')
        chapter_title = best_hit.entity.get('chapter_title', 'N/A')

        print(f" Skor            : {best_hit.distance:.4f}")
        print(f" Bookmark        : {bookmark}")
        print(f" File Name       : {file_name}")
        print(f" Chapter Title   : {chapter_title}")
        print(f" Teks            : {text}")
