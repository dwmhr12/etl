import json
import re
from pathlib import Path


# --- Fungsi Pembersih ---
# deteksi : baris kosong, baris yang isinya karakter aneh, baris yang kebanyakan simbol
def is_noise_line(line):
    """
    Deteksi apakah baris termasuk 'noise':
    - Baris kosong
    - Rasio karakter non-alfanumerik tinggi (dengan pengecualian pola daftar isi seperti titik-titik)
    """
    if not line.strip():
        return True

    # Jangan anggap baris dengan pola daftar isi (.......) sebagai noise
    if re.search(r"\.{5,}", line):  # Banyak titik-titik = kemungkinan besar daftar isi
        return False

    non_alpha = len(re.findall(r"[^a-zA-Z0-9\s]", line))
    ratio = non_alpha / max(1, len(line))
    return ratio > 0.5


def clean_text(text):
    lines = text.splitlines()
    clean_lines = []

    for line in lines:
        line = line.strip()

        # Deteksi baris noise
        if is_noise_line(line):
            continue

        # Buang baris header & penanda administrasi (tapi JANGAN buang DAFTAR ISI)
        if re.search(r"Edisi ke\s*:|Revisi ke\s*:|Tanggal Berlaku|Paraf", line, re.IGNORECASE):
            continue

        # Hapus underscore antar huruf, garis panjang, dan spasi/tab ganda
        line = re.sub(r"(?<=[a-zA-Z])_(?=[a-zA-Z])", "", line)
        line = re.sub(r"[_\-]{3,}", "", line)
        line = re.sub(r"[ \t]+", " ", line)

        clean_lines.append(line)

    cleaned_text = "\n".join(clean_lines)
    cleaned_text = re.sub(r"\n{2,}", "\n", cleaned_text)
    return cleaned_text.strip()


def extract_bookmark_and_title(lines):

    for i, line in enumerate(lines):
        stripped = line.strip().upper()

        # Jika terdeteksi DAFTAR ISI
        if re.match(r"\bDAFTAR\s+ISI\b", stripped):
            return "DAFTAR ISI", ""

        # Jika pola BAB I + Judul
        match = re.match(r"\bBAB[\s\.]*([A-Z0-9]+)[\s:.-]*(.*)", stripped, re.IGNORECASE)
        if match:
            bab = f"BAB {match.group(1)}"
            title = match.group(2).title().strip()
            if not title and i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if not is_noise_line(next_line):
                    title = next_line.title()
            return bab, title

    return None, None



# --- Fungsi Proses JSONL ---
# ambil isi teks content
# bersihin pakek clean_text
# tambahin content_length
def clean_jsonl(input_path, output_path):
    last_bookmark = ""
    last_chapter_title = ""

    with open(input_path, "r", encoding="utf-8") as infile, open(output_path, "w", encoding="utf-8") as outfile:
        for line in infile:
            data = json.loads(line)
            cleaned = clean_text(data["content"])
            data["content"] = cleaned
            data["content_length"] = len(cleaned)

            # Deteksi bookmark & chapter
            lines = cleaned.splitlines()
            bookmark, chapter_title = extract_bookmark_and_title(lines)

            # Kalau tidak ditemukan, pakai yang sebelumnya
            if not bookmark:
                bookmark = last_bookmark
            else:
                last_bookmark = bookmark

            if not chapter_title:
                chapter_title = last_chapter_title
            else:
                last_chapter_title = chapter_title

            data["bookmark"] = bookmark
            data["chapter_title"] = chapter_title

            json.dump(data, outfile, ensure_ascii=False)
            outfile.write("\n")

# --- Eksekusi ---
BASE_DIR = Path(__file__).resolve().parent.parent  # /home/dwmhr/pln-etl

def run_cleansing(input_path=None, **kwargs):
    if input_path is None:
        input_path = BASE_DIR / "data/processed/Kepdir 0306 Kepdir 2023/Kepdir 0306 Kepdir 2023_ekstrak.jsonl"

    input_file = Path(input_path)
    output_dir = input_file.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{input_file.stem}_cleansing.jsonl"
    clean_jsonl(str(input_path), str(output_file))


if __name__ == "__main__":
    run_cleansing()



