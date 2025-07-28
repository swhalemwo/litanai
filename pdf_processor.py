
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

def _ocr_via_screenshot_pipeline(pdf_path, temp_dir):
    """A robust OCR fallback that screenshots each page and returns the raw text."""
    import fitz  # PyMuPDF
    import ocrmypdf
    import os

    print(f"INFO: OCR failed. Initiating screenshot pipeline for {os.path.basename(pdf_path)}.")
    image_files = []
    doc = fitz.open(pdf_path)

    try:
        # 1. Render each page to a high-res PNG
        for i, page in enumerate(doc):
            image_path = os.path.join(temp_dir, f"page_{i:04d}.png")
            pix = page.get_pixmap(dpi=300)
            pix.save(image_path)
            image_files.append(image_path)
    finally:
        doc.close()

    if not image_files:
        print("ERROR: Could not extract any pages as images.")
        return ""

    # 2. Assemble images into a new PDF
    rebuilt_pdf_path = os.path.join(temp_dir, "rebuilt.pdf")
    with fitz.open() as rebuilt_doc:
        for image_file in image_files:
            with fitz.open(image_file) as img_doc:
                rebuilt_doc.insert_pdf(img_doc)
        rebuilt_doc.save(rebuilt_pdf_path)

    # 3. OCR the new, clean PDF to get its text
    text_only_pdf = os.path.join(temp_dir, "text_only.pdf")
    try:
        ocrmypdf.ocr(rebuilt_pdf_path, text_only_pdf, force_ocr=True, deskew=True)
        return extract_text_mupdf(text_only_pdf)
    except Exception as e:
        print(f"ERROR: Screenshot pipeline failed at the final OCR stage: {e}")
        return ""

def extract_text_ocrmypdf(pdf_path):
    """
    Performs OCR on a PDF, using a sidecar-based screenshot fallback.
    Overwrites the original file on success.
    """
    import ocrmypdf
    import shutil
    import os
    import tempfile

    print(f"INFO: Forcing OCR on: {os.path.basename(pdf_path)}")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_output_path = os.path.join(temp_dir, "output.pdf")
        final_text = ""

        try:
            # --- Attempt 1: Direct OCR ---
            ocrmypdf.ocr(pdf_path, temp_output_path, force_ocr=True, deskew=True)
            final_text = extract_text_mupdf(temp_output_path)

        except Exception as e:
            print(f"WARNING: Initial OCR failed: {e}. Falling back to screenshot+sidecar pipeline.")
            # --- Attempt 2: Screenshot + Sidecar ---
            ocr_text = _ocr_via_screenshot_pipeline(pdf_path, temp_dir)

            if ocr_text:
                sidecar_path = os.path.join(temp_dir, "sidecar.txt")
                with open(sidecar_path, 'w', encoding='utf-8') as f:
                    f.write(ocr_text)

                print("INFO: Applying generated text to original PDF via sidecar file.")
                try:
                    ocrmypdf.ocr(
                        pdf_path,             # Input is the original PDF
                        temp_output_path,     # Output is a temporary file
                        sidecar=sidecar_path, # Provide the OCR text
                        force_ocr=True        # Force application of the text layer
                    )
                    final_text = ocr_text # We already have the text
                except Exception as sidecar_e:
                    print(f"ERROR: Sidecar application failed: {sidecar_e}")
                    return ""
            else:
                print("ERROR: Screenshot pipeline produced no text.")
                return ""

        # If any attempt succeeded, overwrite the original file
        if final_text:
            shutil.copy(temp_output_path, pdf_path)
            print(f"SUCCESS: Overwrote original with new OCRed version: {os.path.basename(pdf_path)}")
            return final_text
        else:
            print(f"ERROR: All OCR attempts failed for {pdf_path}.")
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
