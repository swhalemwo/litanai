#!/home/johannes/litanai/bin/python
"""
Main entry point for the litanai application.
"""

import argparse
import os
import pandas as pd
from config import DIR_LIT, DB_NAME
from database import (
    get_clickhouse_client,
    create_littext_table,
    get_existing_keys,
    insert_dataframe,
    write_df_to_sqlite,
    update_sqlite_table,
)
from pdf_processor import get_pdf_text
from llm import query_openai


def update_littext_db(method="mupdf", limit=None):
    """Scans for new PDFs and adds them to the littext database."""
    client = get_clickhouse_client()
    if not client:
        return

    try:
        existing_keys = get_existing_keys(client)
    except Exception as e:
        print(f"Error getting existing keys: {e}")
        print("Please ensure the 'littext' table exists. You can create it with the 'rebuild-db' command.")
        return

    all_pdf_files = {f for f in os.listdir(DIR_LIT) if f.endswith(".pdf")}
    new_pdf_files = sorted(list(all_pdf_files - existing_keys))

    if not new_pdf_files:
        print("No new PDF files to add.")
        return

    if limit:
        new_pdf_files = new_pdf_files[:limit]

    print(f"Found {len(new_pdf_files)} new PDFs to process.")

    data = []
    for pdf_file in new_pdf_files:
        file_path = os.path.join(DIR_LIT, pdf_file)
        print(f"Processing: {pdf_file}")
        text = get_pdf_text(file_path, method)
        if text:
            data.append({"key": pdf_file, "text": text})
    
    print(len(data))
    if data:
        df = pd.DataFrame(data)
        insert_dataframe(client, "littext", df)

def rebuild_littext_db(method="mupdf", limit=None):
    """
    Scans the literature directory, extracts text from PDFs, and rebuilds the littext database.
    """
    client = get_clickhouse_client()
    if not client:
        return

    create_littext_table(client)

    pdf_files = [f for f in os.listdir(DIR_LIT) if f.endswith(".pdf")]
    
    if limit:
        pdf_files = pdf_files[:limit]
    
    if not pdf_files:
        print("No PDF files found in the literature directory.")
        return

    data = []
    for pdf_file in pdf_files:
        file_path = os.path.join(DIR_LIT, pdf_file)
        text = get_pdf_text(file_path, method)
        if text:
            data.append({"key": pdf_file, "text": text})
    
    if data:
        df = pd.DataFrame(data)
        insert_dataframe(client, "littext", df)

def main():
    """Main function to run the litanai application."""
    parser = argparse.ArgumentParser(description="litanai: A tool for literature analysis with AI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Rebuild DB Command ---
    parser_rebuild = subparsers.add_parser("rebuild-db", help="Rebuild the littext database from PDF files.")
    parser_rebuild.add_argument("--method", type=str, default="mupdf", 
                                help="The PDF text extraction method to use (e.g., mupdf, pypdf)." )
    parser_rebuild.add_argument("--limit", type=int, default=None, 
                                help="The maximum number of PDFs to process.")

    # --- Update DB Command ---
    parser_update = subparsers.add_parser("update-db", help="Update the littext database with new PDFs.")
    parser_update.add_argument("--method", type=str, default="mupdf",
                               help="The PDF text extraction method to use.")
    parser_update.add_argument("--limit", type=int, default=None,
                               help="The maximum number of new PDFs to process.")

    args = parser.parse_args()

    if args.command == "rebuild-db":
        rebuild_littext_db(args.method, args.limit)
    elif args.command == "update-db":
        update_littext_db(args.method, args.limit)

if __name__ == "__main__":
    main()
