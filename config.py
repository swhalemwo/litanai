"""
This module contains configuration variables for the litanai project.
"""

import os

# --- Directories ---
PROJ_DIR = "/home/johannes/Dropbox/proj/litanai/"
DIR_LIT = "/home/johannes/Dropbox/readings/"
DIR_PDF = os.path.join(PROJ_DIR, "pdfs") # New directory for downloaded PDFs
DIR_CSV = os.path.join(PROJ_DIR, "csv-files")
DIR_JOURNAL_PICKLES = os.path.join(PROJ_DIR, "journal_pickles") # Assuming this is the intended path

# --- Database ---
DB_NAME = "openai_responses.db"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DB = "litanai"

# --- API Keys ---
# It's recommended to load secrets from a secure vault or environment variables
# For simplicity in this refactoring, we'll continue to use the `pass` command
OPENAI_API_KEY_COMMAND = "pass show openai-key"

# --- LLM ---
LLM_MODEL = "gpt-4o-mini"
