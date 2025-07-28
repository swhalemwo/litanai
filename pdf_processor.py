
# This module handles all PDF processing tasks for the litanai project,
# including text extraction and downloading.

import os
import subprocess
from config import DIR_PDF

# --- PDF Downloading ---

def download_pdf_scidownl(doi, work_id):
    """Downloads a PDF using scidownl."""
    output_path = os.path.join(DIR_PDF, f"{work_id}.pdf")
    if not os.path.exists(output_path):
        cmd = f"scidownl download --doi {doi} --out {output_path}"
        print(f"Executing: {cmd}")
        subprocess.run(cmd, shell=True, check=True)

def download_pdf_scihub(doi, work_id):
    """Downloads a PDF using scihub-py."""
    output_path = os.path.join(DIR_PDF, f"{work_id}.pdf")
    if not os.path.exists(output_path):
        cmd = f"scihub-py -d {doi} -o {output_path}"
        print(f"Executing: {cmd}")
        subprocess.run(cmd, shell=True, check=True)

# --- Text Extraction ---

def extract_text_pypdf(pdf_path):
    """Extracts text from a PDF using pypdf."""
    from pypdf import PdfReader
    try:
        reader = PdfReader(pdf_path)
        return "".join(page.extract_text() for page in reader.pages)
    except Exception as e:
        print(f"Error processing {pdf_path} with pypdf: {e}")
        return ""

def extract_text_mupdf(pdf_path):
    """Extracts text from a PDF using mupdf."""
    import fitz  # PyMuPDF
    try:
        doc = fitz.open(pdf_path)
        return "".join(page.get_text() for page in doc)
    except Exception as e:
        print(f"Error processing {pdf_path} with mupdf: {e}")
        return ""

def extract_text_openparse(pdf_path):
    """Extracts text from a PDF using openparse."""
    import openparse
    try:
        parser = openparse.DocumentParser()
        parsed_content = parser.parse(pdf_path)
        return "".join(node.text for node in parsed_content.nodes)
    except Exception as e:
        print(f"Error processing {pdf_path} with openparse: {e}")
        return ""

def extract_text_pdftotext(pdf_path):
    """Extracts text from a PDF using pdftotext."""
    import pdftotext
    try:
        with open(pdf_path, "rb") as f:
            pdf = pdftotext.PDF(f)
        return "\n\n".join(pdf)
    except Exception as e:
        print(f"Error processing {pdf_path} with pdftotext: {e}")
        return ""

def extract_text_pdfplumber(pdf_path):
    """Extracts text from a PDF using pdfplumber."""
    import pdfplumber
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return "".join(page.extract_text() for page in pdf.pages)
    except Exception as e:
        print(f"Error processing {pdf_path} with pdfplumber: {e}")
        return ""

def extract_text_pdfminer(pdf_path):
    """Extracts text from a PDF using pdfminer."""
    from pdfminer.high_level import extract_text
    try:
        return extract_text(pdf_path)
    except Exception as e:
        print(f"Error processing {pdf_path} with pdfminer: {e}")
        return ""

def extract_text_ocrmypdf(pdf_path):
    """Extracts text from a PDF using ocrmypdf."""
    import ocrmypdf
    output_path = f"{pdf_path}.ocr.pdf"
    try:
        ocrmypdf.ocr(pdf_path, output_path, deskew=True)
        return extract_text_mupdf(output_path)
    except Exception as e:
        print(f"Error processing {pdf_path} with ocrmypdf: {e}")
        return ""

EXTRACTION_METHODS = {
    "pypdf": extract_text_pypdf,
    "mupdf": extract_text_mupdf,
    "openparse": extract_text_openparse,
    "pdftotext": extract_text_pdftotext,
    "pdfplumber": extract_text_pdfplumber,
    "pdfminer": extract_text_pdfminer,
    "ocrmypdf": extract_text_ocrmypdf,
}

def get_pdf_text(pdf_path, method="mupdf"):
    """Extracts text from a PDF using the specified method."""
    if method in EXTRACTION_METHODS:
        return EXTRACTION_METHODS[method](pdf_path)
    else:
        raise ValueError(f"Unknown extraction method: {method}")
