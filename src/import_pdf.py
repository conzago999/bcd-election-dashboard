"""
BCD PDF Import Module
Extracts election data tables from PDF files published by county clerks.
Designed for Boone County IN clerk format, adaptable to other counties.
"""

import pdfplumber
import pandas as pd
import os
import sys
from datetime import datetime


def extract_tables_from_pdf(filepath, pages=None):
    """
    Extract all tables from a PDF file.

    Returns a list of DataFrames, one per table found.
    """
    tables = []

    with pdfplumber.open(filepath) as pdf:
        print(f"PDF: {os.path.basename(filepath)}")
        print(f"Pages: {len(pdf.pages)}")
        print()

        page_range = pages if pages else range(len(pdf.pages))

        for i in page_range:
            if i >= len(pdf.pages):
                break
            page = pdf.pages[i]
            page_tables = page.extract_tables()

            if page_tables:
                for j, table in enumerate(page_tables):
                    if table and len(table) > 1:
                        # Use first row as header
                        df = pd.DataFrame(table[1:], columns=table[0])
                        df["_source_page"] = i + 1
                        tables.append(df)
                        print(f"  Page {i+1}, Table {j+1}: {len(df)} rows, {len(df.columns)} cols")
                        print(f"    Columns: {list(df.columns)}")
            else:
                # Try extracting text if no tables found
                text = page.extract_text()
                if text:
                    print(f"  Page {i+1}: No tables found, but text present ({len(text)} chars)")

    print(f"\nTotal tables extracted: {len(tables)}")
    return tables


def preview_pdf(filepath, max_pages=5):
    """
    Preview the content and structure of a PDF file.
    Useful for understanding the format before building an import pipeline.
    """
    with pdfplumber.open(filepath) as pdf:
        print(f"File: {os.path.basename(filepath)}")
        print(f"Total pages: {len(pdf.pages)}")
        print(f"Metadata: {pdf.metadata}")
        print()

        for i, page in enumerate(pdf.pages[:max_pages]):
            print(f"--- Page {i+1} ---")
            print(f"  Size: {page.width} x {page.height}")

            # Check for tables
            tables = page.extract_tables()
            print(f"  Tables found: {len(tables) if tables else 0}")

            if tables:
                for j, table in enumerate(tables):
                    if table:
                        print(f"    Table {j+1}: {len(table)} rows")
                        # Show first 3 rows
                        for row in table[:3]:
                            print(f"      {row}")
                        if len(table) > 3:
                            print(f"      ... ({len(table)-3} more rows)")

            # Show text extract
            text = page.extract_text()
            if text:
                lines = text.split("\n")
                print(f"  Text lines: {len(lines)}")
                for line in lines[:5]:
                    print(f"    {line}")
                if len(lines) > 5:
                    print(f"    ... ({len(lines)-5} more lines)")
            print()


def pdf_tables_to_excel(filepath, output_path=None):
    """
    Extract all tables from a PDF and save to Excel.
    Each table becomes a sheet. Useful as an intermediate step.
    """
    tables = extract_tables_from_pdf(filepath)

    if not tables:
        print("No tables found in PDF.")
        return None

    if output_path is None:
        base = os.path.splitext(filepath)[0]
        output_path = base + "_extracted.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for i, df in enumerate(tables):
            sheet_name = f"Table_{i+1}_p{df['_source_page'].iloc[0]}"
            df.drop(columns=["_source_page"]).to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"Saved {len(tables)} tables to: {output_path}")
    return output_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python import_pdf.py <path_to_pdf> --preview    Preview PDF structure")
        print("  python import_pdf.py <path_to_pdf> --extract    Extract tables to Excel")
        sys.exit(1)

    filepath = sys.argv[1]

    if "--preview" in sys.argv:
        preview_pdf(filepath)
    elif "--extract" in sys.argv:
        pdf_tables_to_excel(filepath)
    else:
        preview_pdf(filepath)
