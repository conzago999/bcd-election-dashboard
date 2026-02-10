#!/bin/bash
# BCD - Boone County Democrats Election Data Tool
# One-time setup script

echo "========================================"
echo "BCD Election Data Tool - Setup"
echo "========================================"
echo ""

# Check for Python
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is required but not found."
    echo "Install it from: https://www.python.org/downloads/"
    exit 1
fi

echo "Python found: $(python3 --version)"
echo ""

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install pandas openpyxl pdfplumber matplotlib plotly numpy streamlit

echo ""
echo "Initializing database..."
cd src && python3 database.py && cd ..

echo ""
echo "========================================"
echo "Setup complete!"
echo "========================================"
echo ""
echo "To activate the environment:"
echo "  source venv/bin/activate"
echo ""
echo "To preview a data file:"
echo "  python3 src/import_excel.py your_file.xlsx --preview"
echo ""
echo "To import data:"
echo "  python3 src/import_excel.py your_file.xlsx"
echo ""
echo "To launch the dashboard:"
echo "  streamlit run dashboards/app.py"
echo ""
echo "Drop your files in:"
echo "  data/raw_excel/  - for Excel/CSV files"
echo "  data/raw_pdfs/   - for PDF files"
echo ""
