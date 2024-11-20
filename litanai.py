import os
import pymupdf
from openparse import processing, DocumentParser
from pipe import select, take
from itertools import count
from openai import OpenAI
import subprocess
import bibtexparser
import pandas as pd
import numpy as np
import clickhouse_connect
import pdb
import polars as pl
import pyarrow
import json
import tiktoken
import re
import sqlite3
import time
from jutils import *
from globs import *

def get_secret(secret):
    return(
        subprocess.run("pass show " + secret, shell = True,
                       stdout=subprocess.PIPE, text = True).stdout.strip())



# ** parsing bibtex

def gd_bibtex() :

    "basic reading of bibtex files into pandas df"

    FILE_BIB1 = "/home/johannes/Dropbox/sync/dabate/references.bib"
    FILE_BIB2 = "/home/johannes/Dropbox/sync/dabate/references2.bib"

    bibtex_db1 = bibtexparser.parse_file(FILE_BIB1)
    bibtex_db2 = bibtexparser.parse_file(FILE_BIB2)

    bibtex_db = bibtex_db1.entries + bibtex_db2.entries

    data = []

    # pdb.set_trace()

    for entry in bibtex_db:
        author = entry.get("author").value if "author" in entry.fields_dict.keys() else "N/A"
        title = entry.get("title").value if "title" in entry.fields_dict.keys() else "N/A"
        year = entry.get("year").value if "year" in entry.fields_dict.keys() else "N/A"
        doi = entry.get("doi").value if "doi" in entry.fields_dict.keys() else "N/A"
        journal = entry.get("journal").value if "journal" in entry.fields_dict.keys() else "N/A"
        data.append({'key': entry.key, 'author': author, 'title': title, 'year':year, 'doi' : doi,
                     'journal': re.sub(r'\s+', ' ', journal)})

    df_bib = pd.DataFrame(data).replace('N/A', np.nan)
    

    return(df_bib)

def parse_pdf_pymupdf (docpath):
    "parse pdf text with pymupdf"

    print(docpath)

    doc = pymupdf.open(DIR_LIT + docpath)

    l_pages = []

    for page in doc:
        text = page.get_text()
        l_pages.append(text)

        # len(l_pages)

    doc_txt = "\n".join(l_pages)

    return(doc_txt)


def parse_pdf_openparse (docpath):
    "parse text of a pdf with openparse"
    parser = DocumentParser()
    
    parsed_basic_doc = parser.parse(DIR_LIT + docpath)

    l_nodes_text = []

    for node in parsed_basic_doc.nodes:
        l_nodes_text.append(node.text)

        doc_txt = "\n".join(l_nodes_text)

    return(doc_txt)


def gen_initial_db (client) :
    "put existing lit into CH"

    

    DIR_LIT = "/home/johannes/Dropbox/readings/"


    # test_doc = DIR_LIT + "Fasche_2013_history.pdf"

    list_pdfs = [f for f in os.listdir(DIR_LIT) if f[-4:] == ".pdf"]

    l_txt_pymupdf = list(map(parse_pdf_pymupdf, list_pdfs))
    # l_txt_openparse = list(map(parse_pdf_openparse, list_pdfs[0:10])) # use pymupdf, so much faster

    df_text = pd.DataFrame({'key': list_pdfs,
                            'text': l_txt_pymupdf})

    client.command("SET allow_experimental_inverted_index = true;")

    client.command("drop table littext")

    cmd_create_text_tbl = """CREATE TABLE littext (
    `key` String,
    `text` String,
    INDEX inv_idx(text) TYPE inverted(0) GRANULARITY 1
    )
    ENGINE = MergeTree()
    ORDER BY key"""

    client.command(cmd_create_text_tbl)

    client.insert_df('littext', df_text)

def update_littext_db ():

    con_ch = ibis.connect("clickhouse://localhost/litanai")

    tl = con_ch.table('littext')

    # existing texts
    l_xt = tl.select('key').execute()['key'].to_list()

    # existing pdfs
    l_pdfs = [f for f in os.listdir(DIR_LIT) if f[-4:] == ".pdf"]
    
    l_new_pdfs = list(set(l_pdfs) - set(l_xt))
    
    print(len(l_new_pdfs))

    l_txt_pymupdf = lmap(parse_pdf_pymupdf, l_new_pdfs)

    df_text = pd.DataFrame({'key': l_new_pdfs,
                            'key2': lmap(lambda x: x.replace('.pdf', ''), l_new_pdfs),
                            'text': l_txt_pymupdf})
    print(df_text.shape)

    con_ch.insert('littext', df_text)

    # return(df_text)


# client.command("CREATE DATABASE litanai")
# client.command("SHOW DATABASES")


def gd_reltexts (query_reltext):
    "get a df of the relevant texts"
    
    client = clickhouse_connect.get_client(database = "litanai")

    dtx = client.query_df(query_reltext)

    # add count of tokens: shouldn't be longer than 128k (gpt-4o-mini limit)

    if 'text' in dtx.columns:
        encoder = tiktoken.get_encoding("o200k_base")
        dtx.insert(3, 'tokens', dtx.apply(lambda r: len(encoder.encode(r['text'])), axis = 1))

    
    return(dtx)



def qry_oai (key, prompt, text_to_query):
    
    # query testing
    oai_client = OpenAI(api_key = get_secret("openai-key"))
    
      
    # pdb.set_trace()
        
    query = prompt + text_to_query
    
    

    oai_client = OpenAI(api_key = get_secret("openai-key"))
    query_res = oai_client.chat.completions.create(
        messages = [{
            "role": "user",
            "content": query
            # "content": "what is the capital of Belgium?"
            }],
        # model = "gpt-3.5-turbo",
        model = "gpt-4o-mini",
        response_format = {"type" : "json_object"}
        )
    
    res_dict = query_res.to_dict()['choices'][0]['message']['content']

    res_json = json.loads(res_dict)

    return(res_json)

def qry_oai_quotes (key, prompt, text_to_query, proj_name):
    """
    Query OpenAI for quotes (extracted from text) from text_to_query based on prompt.
    writes results do sqlite database to keep track and avoid re-querying after crash.
    Parameters:
        key (str): Identifier for the text.
        prompt (str): Prompt to send to OpenAI.
        text_to_query (str): Text to query.
        proj_name (str): Name of the project to save results.
        Returns:
        dt_res (pd.DataFrame): DataFrame with the results.
    """

    res_json = qry_oai(key, prompt, text_to_query)

    if list(res_json.keys()) == ['pagenumber', 'quote', 'reason']:
        res_json2 = res_json

    elif len(res_json.keys()) == 1:
        res_key = list(res_json.keys())[0]

        # sometimes key is results, sometimes just result
        if res_key in ['results', 'result']:
            res_json2 = res_json[res_key]

        else:
            pdb.set_trace()
        
    dt_res = pd.DataFrame(res_json2)
    write_to_db(dt_res, table_name = proj_name)
    dt_res.insert(0, "key", key)

    return(dt_res)

def write_to_db(dataframe, table_name, db_name='openai_responses.db'):
    """
    Writes a DataFrame to an SQLite database.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame to write to the database.
        db_name (str): The name of the SQLite database file.
        table_name (str): The name of the table to write data to.
    """
    # Connect to the SQLite database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_name)
    
    # Create a table if it doesn't exist
    dataframe.to_sql(table_name, conn, if_exists='append', index=False)
    
    # Close the connection
    conn.close()
    
def edit_db(dataframe, table_name, db_name='openai_responses.db'):
    """
    Edits a DataFrame in an SQLite database.

    Parameters:
        dataframe (pd.DataFrame): The DataFrame to write to the database.
        db_name (str): The name of the SQLite database file.
        table_name (str): The name of the table to write data to.
    """
    # breakpoint()

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # get fields to update
    l_keys_to_update = list(set(dataframe.columns) - set(['key']))
    # get values to update
    l_new_vlus = list(dataframe[l_keys_to_update].iloc[0]) # FIXME: this seems to work only for single rows
    
    
    l_inserts = lmap(lambda x: f"{x} = ?", l_keys_to_update)
    str_inserts = ", ".join(l_inserts)


    update_query = f"UPDATE {table_name} SET {str_inserts} WHERE key = ?"
    print(update_query)
    xx = cursor.execute(update_query, (*l_new_vlus, dataframe['key'][0]))

    conn.commit()
    conn.close()


def qry_oai_assess (key, prompt, text_to_query, proj_name):
    """
    Query OpenAI for assessment of text_to_query based on prompt.
    Saves results to sqlite database to keep track and avoid re-querying after crash.
    Parameters:
        key (str): Identifier for the text.
        prompt (str): Prompt to send to OpenAI.
        text_to_query (str): Text to query.
        proj_name (str): Name of the project to save results.
    Returns:
        dt_res (pd.DataFrame): DataFrame with the results.
    """
    
    
    
    # breakpoint()
    print(key)

    res_json = qry_oai(key, prompt, text_to_query)
    dt_res = pd.DataFrame([res_json])
    dt_res.insert(0, "key", key)
    dt_res.insert(1, "text", text_to_query)
    dt_res.insert(6, "timestamp", time.time())

    write_to_db(dt_res, table_name = proj_name)

    return(dt_res)


def gs_oai_prompt (topic, desc):
    "generate prompt: include extraction steps"
    
    ins_gen = f"""quote every instance that you find literally word for word, don't rephrase anything. include all the text of each instance that is necessary for it to be understood when standing on its own. Do not use ellipses, include each instance from start to finish, even if within there are elements that may seem less relevant. If there is some context surrounding the instance which is necessary to understand the instance, include it in the quote. Rather include too much text than too little. it is important that the point of each instance is clearly understandable on its own.

Return the results as a list of dicts so it can be parsed as json, with each dict having the keys pagenumber, quote, and reason why this instance is relevant. if this text does not concern {topic}, return an empty list.

The text follows below this line:
"""

    prompt_full = desc + "\n\n" + ins_gen
    return(prompt_full)


def litanai (query_reltext, prompt_oai, qry_fun, proj_name, head):
    """
    integrated litreview: first get queries from database

    Args:
        query_reltext (str): sql query to generate a pandas df with columns key (identifier) and text,
                            or the pandas df directly
        prompt_oai (str): prompt to send to open ai
        qry_fun (fun): function to apply to each row (e.g. qry_oai_assessment)
        proj_name (str): project name to save results
        head (bool): flag whether to use only first entries (for debugging)

        Returns:
            nothing
    """
    
    # breakpoint()
    
    ## if input is pandas.df, use that, else generate it from query
    if isinstance(query_reltext, str):
        dt_reltexts = gd_reltexts(query_reltext)
    elif isinstance(query_reltext, pd.core.frame.DataFrame):
        dt_reltexts = query_reltext


    print(dt_reltexts.shape)

    # check which works have been checked already
    con_sqlite = ibis.connect("sqlite://openai_responses.db")
    tproj = con_sqlite.table(proj_name)
    keys_in_proj = tproj.select('key').distinct().execute()['key'].to_list()

    con_ch = ibis.connect("clickhouse://localhost/litanai")
    tcree = con_ch.table('cree')
    keys_in_ch = tcree.select('work_id').distinct().execute()['work_id'].to_list()

    keys_to_yeet = list(set(keys_in_proj + keys_in_ch))

    dt_reltexts_fltrd = dt_reltexts[~dt_reltexts['key'].isin(keys_to_yeet)]
    dt_reltexts_fltrd = dt_reltexts_fltrd[dt_reltexts_fltrd['tokens'] < 1e5]

    # & ~dt_reltexts['tokens'] < 1e5]

    print(dt_reltexts_fltrd.shape)
        
    # raise RuntimeError("it's time to stop.")

    if head:
        dt_reltexts_fltrd = dt_reltexts_fltrd.head()
        
    l_res = (dt_reltexts_fltrd
             .apply(lambda row: qry_fun(row['key'], prompt_oai, row['text'], proj_name), axis = 1))
        
    # combine results
    l_res_cbnd = [res for res in l_res if not res.empty]

    dt_res_cbnd = pd.concat(l_res_cbnd)

    dt_res_cbnd.to_csv(f"{PROJ_DIR}res/res_{proj_name}.csv")
    print('done')
    return(dt_res_cbnd)


# if __name__ == "__main__":


# * main

# ** private museum influence

# update_littext_db()

# qry_oai(dt_reltexts['key'][0], prompt_oai, dt_reltexts['text'][0])

# qry_oai('somekey', "you look at a text that contains some words. return all words that refer to colors. put them in a json list", "red apple blue car dragon")
