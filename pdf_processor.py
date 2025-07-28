
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
    """
    Performs OCR on a PDF, attempting to repair it if initial OCR fails.
    It forces OCR on all pages and overwrites the original file on success.
    """
    import ocrmypdf
    import shutil
    import os
    import subprocess

    print(f"INFO: Forcing OCR on: {os.path.basename(pdf_path)}")

    temp_output_path = f"{pdf_path}.__ocrtemp__.pdf"

    try:
        # First attempt at OCR
        ocrmypdf.ocr(
            pdf_path,
            temp_output_path,
            force_ocr=True,
            deskew=True,
        )
    except Exception as e:
        # Check if the error is the Ghostscript one we know how to handle
        if 'Ghostscript' in str(e):
            print(f"WARNING: Ghostscript failed on initial OCR attempt for {os.path.basename(pdf_path)}. Attempting to repair PDF first.")
            repaired_path = f"{pdf_path}.__repaired__.pdf"
            try:
                # Use Ghostscript to re-distill/repair the PDF
                subprocess.run([
                    'gs',
                    '-o', repaired_path,
                    '-sDEVICE=pdfwrite',
                    '-dPDFSETTINGS=/default',
                    pdf_path
                ], check=True, capture_output=True, text=True) # Capture output to hide gs messages unless error

                print("INFO: PDF repair successful. Retrying OCR on repaired file.")
                # Retry OCR on the repaired file
                ocrmypdf.ocr(
                    repaired_path,
                    temp_output_path,
                    force_ocr=True,
                    deskew=True,
                )
            except Exception as repair_e:
                print(f"ERROR: OCR failed even after repair attempt for {pdf_path}: {repair_e}")
                # Clean up temporary files
                if os.path.exists(repaired_path):
                    os.remove(repaired_path)
                if os.path.exists(temp_output_path):
                    os.remove(temp_output_path)
                return ""
            finally:
                 if os.path.exists(repaired_path):
                    os.remove(repaired_path)
        else:
            # It's a different, unexpected error
            print(f"ERROR: An unexpected OCR error occurred for {pdf_path}: {e}")
            if os.path.exists(temp_output_path):
                os.remove(temp_output_path)
            return ""

    # If we reach here, one of the OCR attempts was successful
    shutil.move(temp_output_path, pdf_path)
    print(f"SUCCESS: Overwrote original with OCRed version: {os.path.basename(pdf_path)}")
    return extract_text_mupdf(pdf_path)


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
    """Extracts text from a PDF, falling back to OCR if needed."""
    if method not in EXTRACTION_METHODS:
        raise ValueError(f"Unknown extraction method: {method}")

    # First, try the specified primary method
    text = EXTRACTION_METHODS[method](pdf_path)

    # If the text is empty or just whitespace, fall back to OCR
    if not text or text.isspace():
        print(f"No text found with '{method}'. Falling back to OCR for: {os.path.basename(pdf_path)}")
        # We use extract_text_mupdf to get the text from the OCRed file
        text = extract_text_ocrmypdf(pdf_path)

    return text
