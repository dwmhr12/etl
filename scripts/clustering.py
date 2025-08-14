from pymilvus import Collection, connections
from sentence_transformers import SentenceTransformer, util

# Step 1: Koneksi ke Milvus
connections.connect("default", host="localhost", port="19530")

# Step 2: Load model embedding
model = SentenceTransformer("intfloat/multilingual-e5-large", device='cpu')

# Step 3: Load collection dari Milvus
collection = Collection("pln_embeddings_simplified")
collection.load()

# Step 4: Fungsi untuk mencari dan mengelompokkan hasil berdasarkan kemiripan
def search_and_group_chunks(query, top_k=20, similarity_threshold=0.75):
    print(f"Pencarian: '{query}' (Threshold antar paragraf: {similarity_threshold*100}%)")
    print("=" * 100)

    # Encode query
    query_embedding = model.encode([query]).tolist()

    # Lakukan pencarian ke Milvus
    search_result = collection.search(
        data=query_embedding,
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {"nprobe": 10}},
        limit=top_k,
        output_fields=["text", "bookmark", "file_name", "chapter_title"]
    )

    # Ambil hasil dan simpan ke dalam list
    results = search_result[0]
    chunks = []
    embeddings = []

    for hit in results:
        chunks.append({
            "text": hit.entity.get("text", ""),
            "bookmark": hit.entity.get("bookmark", "N/A"),
            "file_name": hit.entity.get("file_name", "N/A"),
            "chapter_title": hit.entity.get("chapter_title", "N/A"),
            "score": hit.distance
        })
        # Re-encode karena kita perlu embedding teks untuk clustering
        embeddings.append(model.encode(hit.entity.get("text", "")))

    # Kelompokkan berdasarkan similarity
    groups = []
    visited = [False] * len(chunks)

    for i in range(len(chunks)):
        if visited[i]:
            continue
        group = [chunks[i]]
        visited[i] = True
        for j in range(i + 1, len(chunks)):
            if not visited[j]:
                sim = util.cos_sim(embeddings[i], embeddings[j]).item()
                if sim >= similarity_threshold:
                    group.append(chunks[j])
                    visited[j] = True
        groups.append(group)

    # Cetak hasil pengelompokan
    for idx, group in enumerate(groups):
        print(f"\n Group Topik #{idx + 1} (total {len(group)} paragraf)")
        print("-" * 100)
        for para in group:
            print(f" Skor         : {para['score']:.4f}")
            print(f" File Name    : {para['file_name']}")
            print(f" Bookmark     : {para['bookmark']}")
            print(f" Chapter      : {para['chapter_title']}")
            print(f" Teks         : {para['text']}\n")

# ==========================
# PEMANGGILAN FUNGSI
# ==========================
if __name__ == "__main__":
    query = "klasifikasi PLN talent profile"
    search_and_group_chunks(query)
