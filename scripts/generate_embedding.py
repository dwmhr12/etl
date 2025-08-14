import json
import time  
from pathlib import Path
from sentence_transformers import SentenceTransformer

BASE_DIR = Path(__file__).resolve().parent.parent

def generate_embeddings():
    start_time = time.time()  

    input_path = BASE_DIR / "data/processed/Kepdir 0306 Kepdir 2023_v7/Kepdir 0306 Kepdir 2023_ekstrak_chunked.jsonl"
    
    if not input_path.exists():
        raise FileNotFoundError(f"Input path not found: {input_path}")
    
    output_path = input_path.with_name(f"{input_path.stem}_embedding.jsonl")

    with open(input_path, "r", encoding="utf-8") as f:
        documents = [json.loads(line) for line in f]

    texts = [doc["text"] for doc in documents]
    file_names = [doc.get("file_name", "") for doc in documents]
    page_numbers = [doc.get("page_number", 0) for doc in documents]
    bookmarks = [doc.get("bookmark", "") for doc in documents]
    text_lengths = [doc.get("text_length", len(doc["text"])) for doc in documents]
    has_tables_flags = [doc.get("has_tables", False) for doc in documents]
    chapter_titles = [doc.get("chapter_title", "") for doc in documents]

    print("Generating embeddings...")
    model = SentenceTransformer("BAAI/bge-m3", device="cpu")
    embeddings = model.encode(
        texts,
        batch_size=4,  # atau 4 jika RAM benar-benar terbatas
        show_progress_bar=True
    ).tolist()

    with open(output_path, "w", encoding="utf-8") as out_f:
        for i in range(len(texts)):
            item = {
                "embedding": embeddings[i],
                "text": texts[i],
                "file_name": file_names[i],
                "page_number": page_numbers[i],
                "bookmark": bookmarks[i],
                "text_length": text_lengths[i],
                "has_tables": has_tables_flags[i],
                "chapter_title": chapter_titles[i],
            }
            json.dump(item, out_f, ensure_ascii=False)
            out_f.write("\n")

    end_time = time.time()  # ⏱️ Selesai hitung waktu
    elapsed_time = end_time - start_time
    print(f"Embeddings saved to {output_path}")
    print(f"[DONE] Embedding generation completed in {elapsed_time:.2f} seconds.")

    return str(output_path)

if __name__ == "__main__":
    generate_embeddings()
