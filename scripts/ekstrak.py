import pdfplumber
import re
import json
from pathlib import Path
import logging

def extract_pdf_detailed_bookmarks(file_path, output_path):
    output_data = []
    filename = Path(file_path).name
    current_bookmark = None

    bookmark_patterns = [
        (r"^(BAB|Bab|bab)\s+([IVXLCDM]+|\d+)(\.|:)?\s*(.*)$", "main_chapter"),
        (r"^(LAMPIRAN|Lampiran|lampiran)\s+([IVXLCDM]+|\d+|\w+)(\.|:)?\s*(.*)$", "appendix"),
        (r"^(BAGIAN|Bagian|bagian)\s+([IVXLCDM]+|\d+)(\.|:)?\s*(.*)$", "section"),
        (r"^(\d+\.\d+\.?\d*)\s+(.+)$", "subsection"),
        (r"^([A-Z]\.|[a-z]\.)\s+(.+)$", "lettered_section"),
        # Removed overly generic title_section pattern
        # (r"^([A-Z][A-Z\s]{8,})$", "title_section")
    ]

    with pdfplumber.open(file_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            print(f"Processing page {page_num}...")

            text = page.extract_text()
            tables = page.extract_tables()

            if not text:
                continue

            lines = text.split("\n")
            table_index = 0

            # Detect if this is a TOC page
            toc_like_lines = sum(
                1 for line in lines
                if re.match(r"^(BAB|Bab|bab)\s+[IVXLCDM\d]+.*\.+\s+\d+$", line) or
                   re.match(r"^\d+(\.\d+)+.*\.+\s+\d+$", line)
            )
            is_toc_page = toc_like_lines >= 3

            table_lines_to_skip = set()
            for table in tables:
                if table and len(table) > 1:
                    for row in table[1:]:
                        non_empty_cells = [cell.strip() for cell in row if cell and cell.strip()]
                        if len(non_empty_cells) >= 2:
                            for line in lines:
                                line_clean = line.strip()
                                if line_clean:
                                    cells_in_line = sum(1 for cell in non_empty_cells if cell in line_clean)
                                    if cells_in_line == len(non_empty_cells) and len(line_clean.split()) <= len(non_empty_cells) + 2:
                                        table_lines_to_skip.add(line_clean)

            page_content = []
            i = 0
            bookmark_found_on_page = False

            while i < len(lines):
                line = lines[i].strip()

                # Normalisasi kesalahan umum
                line = re.sub(r"\bBABI\b", "BAB I", line, flags=re.IGNORECASE)

                if not line:
                    i += 1
                    continue

                bookmark_found = False
                if not is_toc_page:
                    for pattern, bookmark_type in bookmark_patterns:
                        match = re.match(pattern, line)
                        if match:
                            if bookmark_type == "main_chapter":
                                chapter_type = match.group(1).upper()
                                chapter_num = match.group(2)
                                title = match.group(4).strip() if len(match.groups()) >= 4 and match.group(4) else ""

                                if not title and i + 1 < len(lines):
                                    next_line = lines[i + 1].strip()
                                    if next_line and not any(re.match(p[0], next_line) for p in bookmark_patterns):
                                        title = next_line

                                current_bookmark = f"{chapter_type} {chapter_num}" + (f" {title}" if title else "")

                            elif bookmark_type == "appendix":
                                app_type = match.group(1).upper()
                                app_num = match.group(2)
                                title = match.group(4).strip() if len(match.groups()) >= 4 and match.group(4) else ""
                                current_bookmark = f"{app_type} {app_num}" + (f" {title}" if title else "")

                            elif bookmark_type == "subsection":
                                if current_bookmark and current_bookmark.startswith("BAB"):
                                    pass
                                else:
                                    num = match.group(1)
                                    title = match.group(2).strip()
                                    current_bookmark = f"{num} {title}"

                            elif bookmark_type == "lettered_section":
                                pass  # Optional: you could handle A. / a. sections if needed

                            bookmark_found = True
                            bookmark_found_on_page = True
                            break

                if bookmark_found:
                    page_content.append(line)
                    i += 1
                    continue

                table_match = re.search(r"[Tt]abel\s*(\d+)", line, re.IGNORECASE)

                if table_match and table_index < len(tables):
                    page_content.append(line)
                    table = tables[table_index]
                    if table and len(table) > 1:
                        headers = [h.strip() if h else f"Col{idx+1}" for idx, h in enumerate(table[0])]
                        for row_idx, row in enumerate(table[1:], start=1):
                            row_items = []
                            for header, cell in zip(headers, row):
                                cell_value = cell.strip() if cell else ""
                                if cell_value:
                                    row_items.append(f"{header}: {cell_value}")
                            if row_items:
                                page_content.append(f"{row_idx}. {'; '.join(row_items)}")

                    table_index += 1
                else:
                    if line not in table_lines_to_skip:
                        page_content.append(line)

                i += 1

            # Fallback: jika tidak ada bookmark eksplisit tapi ada sinyal kuat
            if not bookmark_found_on_page:
                lowered = text.lower()
                if "daftar isi" in lowered:
                    current_bookmark = "DAFTAR ISI"
                elif "bab i" in lowered and "pendahuluan" in lowered:
                    current_bookmark = "BAB I PENDAHULUAN"

            if page_content:
                page_entry = {
                    "filename": filename,
                    "page_number": page_num,
                    "bookmark": current_bookmark,
                    "content": "\n".join(page_content),
                    "content_length": len("\n".join(page_content)),
                    "has_tables": len([line for line in page_content if re.search(r"[Tt]abel\s*\d+", line)]) > 0
                }
                output_data.append(page_entry)

    with open(output_path, "w", encoding="utf-8") as f:
        for entry in output_data:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print(f"Extraction completed. {len(output_data)} pages processed.")
    print(f"Output saved to: {output_path}")

    bookmarks = set(entry["bookmark"] for entry in output_data if entry["bookmark"])
    print(f"Found {len(bookmarks)} unique bookmarks:")
    for bookmark in sorted(bookmarks):
        print(f"  - {bookmark}")

    return output_data

BASE_DIR = Path(__file__).resolve().parent.parent  # /home/dwmhr/pln-etl

def run_ekstrak():
    file_path = BASE_DIR / "data/raw/PDF_ATURAN_HC/EDIR-2023.0050-Peraturan Pelaksana Standar Prosedur Manajemen Talenta dan Pegawai.pdf"
    logging.info("=== Mulai ekstraksi PDF ===")
    try:
        filename = file_path.name
        name_without_ext = filename.rsplit(".", 1)[0]
        output_folder = BASE_DIR / "data/processed" / name_without_ext
        output_folder.mkdir(parents=True, exist_ok=True)
        output_file = output_folder / f"{name_without_ext}_ekstrak.jsonl"
        extract_pdf_detailed_bookmarks(str(file_path), str(output_file))
        logging.info(f"=== Ekstraksi selesai untuk {filename} ===")
    except Exception as e:
        logging.exception(f"Terjadi error saat ekstraksi {file_path}: {e}")
        raise

if __name__ == "__main__":
    run_ekstrak()


