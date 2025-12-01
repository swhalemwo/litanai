
# This module handles all PDF processing tasks for the litanai project,
# including text extraction and downloading.

import os
import subprocess
from config import DIR_PDF
from time import sleep

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





def re_render_pdf_with_ghostscript(pdf_path):
    """
    Attempts to re-render a PDF using Ghostscript, overwriting the original file.
    This can fix structural corruption by effectively 'printing' the PDF to a new file.
    Returns True if re-rendering was successful, False otherwise.
    """
    import subprocess
    import os
    import shutil

    print(f"INFO: Attempting to re-render {os.path.basename(pdf_path)} with Ghostscript.")
    temp_re_rendered_path = f"{pdf_path}.__gs_re_rendered__.pdf"

    try:
        subprocess.run(
            [
                'gs',
                '-o', temp_re_rendered_path,
                '-sDEVICE=pdfwrite',
                '-dCompatibilityLevel=1.4', # Use a common compatibility level
                '-dPDFSETTINGS=/prepress',  # High quality output
                '-dNOPAUSE',
                '-dBATCH',
                pdf_path
            ],
            check=True, capture_output=True, text=True
        )
        shutil.copy(temp_re_rendered_path, pdf_path)
        print(f"SUCCESS: {os.path.basename(pdf_path)} re-rendered by Ghostscript.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Ghostscript re-rendering failed for {os.path.basename(pdf_path)}: {e.stderr}")
        return False
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during Ghostscript re-rendering for {os.path.basename(pdf_path)}: {e}")
        return False
    finally:
        if os.path.exists(temp_re_rendered_path):
            os.remove(temp_re_rendered_path)

EXTRACTION_METHODS = {
    "pypdf": extract_text_pypdf,
    "mupdf": extract_text_mupdf,
    "openparse": extract_text_openparse,
    "pdftotext": extract_text_pdftotext,
    "pdfplumber": extract_text_pdfplumber,
    "pdfminer": extract_text_pdfminer,

}

def get_pdf_text(pdf_path, method="mupdf"):
    """Extracts text from a PDF, falling back to OCR if needed."""
    if method not in EXTRACTION_METHODS:
        raise ValueError(f"Unknown extraction method: {method}")

    # First, try the specified primary method
    try:
        text = EXTRACTION_METHODS[method](pdf_path)


    except Exception:
        print("somethign wrong")

    print(type(text))
    # print("hi:" + text)
    # print(len(text))



            
            
    # If the text is empty or just whitespace, fall back to OCR
    if not text: # or text.isspace():
        print(f"No text found with '{method}'. Falling back to Tesseract OCR for: {os.path.basename(pdf_path)}")
        text = _ocr_via_tesseract(pdf_path)

    return text

def _ocr_via_tesseract(pdf_path):
    """
    Performs OCR on a PDF using Tesseract, creates a searchable PDF, 
    and overwrites the original file. This version is aggressively 
    optimized for smaller file size.
    """
    import fitz  # PyMuPDF
    import subprocess
    import os
    import tempfile
    import shutil

    print(f"INFO: Initiating Tesseract OCR pipeline for {os.path.basename(pdf_path)}.")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        doc = fitz.open(pdf_path)
        ocred_page_pdfs = []

        try:
            # 1. OCR each page and create a single-page PDF for each
            for i, page in enumerate(doc):
                image_path = os.path.join(temp_dir, f"page_{i:04d}.jpg")
                output_base = os.path.join(temp_dir, f"page_{i:04d}")
                
                # Render to JPEG at 150 DPI with a specific quality
                pix = page.get_pixmap(dpi=150)
                pix.save(image_path, "jpeg", jpg_quality=80)

                try:
                    # Tesseract outputs a PDF with the text layer
                    subprocess.run(
                        ['tesseract', image_path, output_base, '-l', 'eng', 'pdf'],
                        check=True, capture_output=True, text=True
                    )
                    ocred_page_pdfs.append(f"{output_base}.pdf")
                except subprocess.CalledProcessError as e:
                    print(f"WARNING: Tesseract OCR failed for page {i+1}: {e.stderr}")
                except FileNotFoundError:
                    print("ERROR: Tesseract is not installed or not in PATH.")
                    return ""
        finally:
            doc.close()

        if not ocred_page_pdfs:
            print("ERROR: No pages were successfully OCRed.")
            return ""

        # 2. Combine the OCRed pages into a single PDF
        combined_pdf_path = os.path.join(temp_dir, "combined.pdf")
        with fitz.open() as final_doc:
            for pdf_page_path in ocred_page_pdfs:
                with fitz.open(pdf_page_path) as page_doc:
                    final_doc.insert_pdf(page_doc)
            final_doc.save(combined_pdf_path)

        # 3. Post-process with Ghostscript for final size reduction
        final_pdf_path = os.path.join(temp_dir, "final_ocr.pdf")
        try:
            subprocess.run(
                [
                    'gs',
                    '-sDEVICE=pdfwrite',
                    '-dCompatibilityLevel=1.4',
                    '-dPDFSETTINGS=/ebook',
                    '-dNOPAUSE',
                    '-dQUIET',
                    '-dBATCH',
                    f'-sOutputFile={final_pdf_path}',
                    combined_pdf_path
                ],
                check=True, capture_output=True, text=True
            )
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print(f"WARNING: Ghostscript optimization failed: {e}. Using unoptimized PDF.")
            shutil.copy(combined_pdf_path, final_pdf_path)


        # 4. Overwrite the original file and return the text
        if os.path.exists(final_pdf_path):
            shutil.copy(final_pdf_path, pdf_path)
            print(f"SUCCESS: Overwrote original with new OCRed version: {os.path.basename(pdf_path)}")
            return extract_text_mupdf(pdf_path)
        else:
            print("ERROR: Final OCRed PDF was not created.")
            return ""
