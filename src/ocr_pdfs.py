"""
BCD OCR Pipeline
Converts scanned/image-based PDFs to text using Tesseract OCR,
then passes the text through the standard parser.
"""

import subprocess
import os
import sys
import tempfile
from pathlib import Path

# Add tesseract and poppler to PATH
os.environ["PATH"] = "/opt/homebrew/bin:" + os.environ.get("PATH", "")

import pytesseract
from pdf2image import convert_from_path
from PIL import Image

from parse_all_pdfs import parse_pdf_universal, load_parsed_into_db


def ocr_pdf_to_text(filepath, dpi=300):
    """
    Convert a scanned PDF to text using OCR.

    1. Converts each PDF page to an image (via poppler/pdftoppm)
    2. Runs Tesseract OCR on each image
    3. Returns the full text
    """
    filename = os.path.basename(filepath)
    print(f"  OCR processing: {filename}")
    print(f"  Converting PDF pages to images (DPI={dpi})...")

    # Convert PDF to images
    images = convert_from_path(filepath, dpi=dpi)
    print(f"  {len(images)} pages converted to images")

    all_text = []
    for i, image in enumerate(images):
        # OCR each page
        text = pytesseract.image_to_string(image)
        all_text.append(text)
        if (i + 1) % 10 == 0 or (i + 1) == len(images):
            print(f"  OCR progress: {i+1}/{len(images)} pages")

    full_text = "\n".join(all_text)
    return full_text


def ocr_pdf_and_save(filepath, output_dir=None):
    """
    OCR a PDF and save the extracted text to a .txt file.
    Returns the path to the text file.
    """
    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(os.path.dirname(filepath)), "processed")

    os.makedirs(output_dir, exist_ok=True)

    base = os.path.splitext(os.path.basename(filepath))[0]
    txt_path = os.path.join(output_dir, f"{base}_ocr.txt")

    # Skip if already OCR'd
    if os.path.exists(txt_path) and os.path.getsize(txt_path) > 100:
        print(f"  Already OCR'd: {txt_path}")
        with open(txt_path, "r") as f:
            return f.read(), txt_path

    text = ocr_pdf_to_text(filepath)

    with open(txt_path, "w") as f:
        f.write(text)

    print(f"  Saved OCR text to: {txt_path} ({len(text):,} chars)")
    return text, txt_path


def process_scanned_pdfs(pdf_dir="data/raw_pdfs"):
    """
    Find all scanned PDFs and OCR them.
    """
    import pdfplumber

    scanned_files = []

    for f in sorted(os.listdir(pdf_dir)):
        if not f.endswith(".pdf"):
            continue
        filepath = os.path.join(pdf_dir, f)
        try:
            with pdfplumber.open(filepath) as pdf:
                page = pdf.pages[0]
                text = page.extract_text()
                if not text or len(text.strip()) < 20:
                    scanned_files.append(filepath)
        except:
            pass

    print(f"Found {len(scanned_files)} scanned PDFs to OCR:")
    for f in scanned_files:
        print(f"  {os.path.basename(f)}")
    print()

    results = []
    for filepath in scanned_files:
        print(f"\n{'='*60}")
        print(f"Processing: {os.path.basename(filepath)}")
        try:
            text, txt_path = ocr_pdf_and_save(filepath)

            if len(text.strip()) < 50:
                print(f"  WARNING: OCR produced very little text")
                results.append({"file": os.path.basename(filepath), "status": "OCR failed - no text"})
                continue

            # Now try to parse the OCR'd text using our standard parser
            # We need to write a temp file that pdfplumber can't read but our text parser can
            print(f"  OCR produced {len(text):,} characters")
            print(f"  First 500 chars:")
            print(f"  {text[:500]}")

            results.append({
                "file": os.path.basename(filepath),
                "status": "OCR complete",
                "text_file": txt_path,
                "chars": len(text),
            })

        except Exception as e:
            print(f"  ERROR: {e}")
            results.append({"file": os.path.basename(filepath), "status": f"ERROR: {str(e)[:80]}"})

    return results


if __name__ == "__main__":
    if "--all" in sys.argv:
        process_scanned_pdfs()
    elif len(sys.argv) > 1:
        filepath = sys.argv[1]
        text, txt_path = ocr_pdf_and_save(filepath)
        print(f"\nFirst 1000 chars of OCR output:")
        print(text[:1000])
    else:
        print("Usage:")
        print("  python ocr_pdfs.py --all              OCR all scanned PDFs")
        print("  python ocr_pdfs.py <file.pdf>          OCR a single PDF")
