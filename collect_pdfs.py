
import csv
import glob
import gzip
import json
import os
import time

import pandas as pd

import clickhouse_connect

import pickle
import subprocess


from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, MetaData, create_engine, text, select, tuple_, func, desc
from clickhouse_sqlalchemy import Table, make_session, get_declarative_base, types, engines
from requests import Session


from globs import DIR_CSV, DIR_JOURNAL_PICKLES, DIR_PDF


def dl_doi_ppb(doi, id_work):
    "generate command for PyPaperBot"

    if f"{doi}.pdf" not in os.listdir(DIR_PDF):

        cmd = f"scidownl download --doi {doi} --out {os.path.join(DIR_PDF, id_work)}.pdf"
    
        print(cmd)
        subprocess.run(cmd, shell=True)

# dl_doi_ppb("https://doi.org/10.1002/9781405164061.ch25", "W1849566444")
