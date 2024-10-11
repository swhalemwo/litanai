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
        data.append({'key': entry.key, 'author': author, 'title': title, 'year':year, 'doi' : doi})

    df_bib = pd.DataFrame(data).replace('N/A', np.nan)
    

    return(df_bib)

def parse_pdf_pymupdf (docpath):
    "parse pdf text with pymupdf"

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






# client.command("CREATE DATABASE litanai")
# client.command("SHOW DATABASES")


# if __name__ == "__main__":

def gd_reltexts (query_reltext):
    "get a df of the relevant texts"
    
    client = clickhouse_connect.get_client(database = "litanai")

    dtx = client.query_df(query_reltext)

    # add count of tokens: shouldn't be longer than 128k (gpt-4o-mini limit)
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

    try:
        res_json = json.loads(res_dict)['results']

    except:
        pdb.set_trace()
        
    dt_res = pd.DataFrame(res_json)
    dt_res.insert(0, "key", key)

    return(dt_res)


# * main



query_reltext = "".join(["select key, length(text) AS len, ", 
                     "(length(text) - length(replace(text, 'private museum', ''))) /",
                     "length('private museum') AS n_occur, text ", 
                     " from littext where text LIKE '%private museum%'",
                     " AND n_occur > 10",
                     " order by n_occur DESC"])


dt_reltexts = gd_reltexts(query_reltext)
pl.DataFrame(dt_reltexts)


dt_reltexts[dt_reltexts['tokens'] < 100000]

prompt = """you will read a long text. the text is in some way about private art museums, a new form of museums started by wealthy collectors. you have to find every instance in this text about how private art museums have (or have not) an effect on the arts, for example that the artists after being exhibted experience a boost to their career, increase their chances of canonization or consecration, are more likely to be raise higher prices at auctions or are more likely to be exhibited by other museums or institutions. Any impact that private museums leave in the field of artistic production.

quote every instance that you find literally word for word, don't rephrase anything. include all the text of each instance that is necessary for it to be understood when standing on its own. Do not use ellipses, include each instance from start to finish, even if inbetween there are elements that may seem less relevant. If there is some context surrounding the quote which is necessary to understand it, include it in the quote. Rather include too much text than too little, it is important that the point of each instance is clearly understandable, and that a quote convey its message on its own. 

Return the results as a list of dicts so it can be parsed as json, with each dict having the keys pagenumber, quote, and reason why this instance is relevant. if this text does not concern canonization/artist careers, return an empty list.

The next follows below this line:
"""


# single query
jj = qry_oai("Brown_2019_private.pdf", prompt, dt_reltexts.iloc[0]['text'])


l_res = (dt_reltexts[dt_reltexts['tokens'] < 100000]
         .apply(lambda row: qry_oai(row['key'], prompt, row['text']), axis = 1))




l_res = dt_reltexts.head().apply(lambda row: qry_oai(row['key'], prompt, row['text']), axis = 1)

l_res_cbnd = [res for res in l_res if not res.empty]

dt_res_cbnd = pd.concat(l_res_cbnd)

PROJ_DIR = "/home/johannes/Dropbox/proj/litanai/"

dt_res_cbnd.to_csv(PROJ_DIR + "res.csv")


