import json
import time  # <-- Tambahkan ini

# Fungsi untuk memotong teks berdasarkan jumlah token
def chunk_by_token(text, tokenizer, max_tokens=450, overlap=50):
    inputs = tokenizer(
        text,
        return_overflowing_tokens=True,
        max_length=max_tokens,
        stride=overlap,
        truncation=True,
        add_special_tokens=False
    )

    chunks = []
    for chunk in inputs["input_ids"]:
        decoded = tokenizer.decode(chunk, skip_special_tokens=True).strip()
        chunks.append(decoded)
    
    return chunks

# Fungsi utama untuk memproses file JSONL
def chunk_jsonl_by_token(input_path, output_path, tokenizer, max_tokens=450, overlap=50):
    with open(input_path, "r", encoding="utf-8") as infile, open(output_path, "w", encoding="utf-8") as outfile:
        for line in infile:
            data = json.loads(line)
            content = data.get("content", "")
            file_name = data.get("filename", "")
            bookmark = data.get("bookmark", "")
            chapter_title = data.get("chapter_title", "")
            page_number = data.get("page_number", 0)
            has_tables = data.get("has_tables", False)

            # Proses chunking
            chunks = chunk_by_token(content, tokenizer, max_tokens=max_tokens, overlap=overlap)

            for i, chunk in enumerate(chunks):
                chunk_data = {
                    "chunk_id": f"{file_name}_{bookmark}_{i}",
                    "text": chunk,
                    "file_name": file_name,
                    "bookmark": bookmark,
                    "chapter_title": chapter_title,
                    "page_number": page_number,
                    "has_tables": has_tables,
                    "text_length": len(chunk)
                }
                json.dump(chunk_data, outfile, ensure_ascii=False)
                outfile.write("\n")

# Fungsi ini dipanggil dari DAG
def run_chunk():
    from transformers import AutoTokenizer
    from pathlib import Path

    start_time = time.time()  

    print("[INFO] Loading tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained("BAAI/bge-m3")
    print("[INFO] Tokenizer loaded.")

    input_path = Path("/home/dwmhr/pln/data/processed/Kepdir 0306 Kepdir 2023_v7/Kepdir 0306 Kepdir 2023_ekstrak_cleansing.jsonl")
    output_path = input_path.parent / f"{input_path.stem.replace('_cleansing','')}_chunked.jsonl"

    print(f"[INFO] Input path: {input_path}")
    print(f"[INFO] Output path: {output_path}")

    chunk_jsonl_by_token(str(input_path), str(output_path), tokenizer, max_tokens=450, overlap=50)

    end_time = time.time() 
    elapsed_time = end_time - start_time

    print(f"[DONE] Chunking complete in {elapsed_time:.2f} seconds.")

if __name__ == "__main__":
    run_chunk()
