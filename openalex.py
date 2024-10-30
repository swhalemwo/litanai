import csv
import glob
import gzip
import json
import os
import time

import pandas as pd
import pyalex
from pyalex import Works, Authors, Sources, Institutions, Topics, Concepts, Publishers, Funders, config, invert_abstract

from flatten_openalex_jsonl import flatten_works, init_dict_writer

import clickhouse_connect
import pickle
import subprocess


from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, MetaData, create_engine, text, select, tuple_, func, desc
from clickhouse_sqlalchemy import Table, make_session, get_declarative_base, types, engines
from requests import Session


from globs import DIR_CSV, DIR_JOURNAL_PICKLES

uri = 'clickhouse+native://localhost/litanai'
engine = create_engine(uri)
session = make_session(engine)
metadata = MetaData()

metadata.reflect(bind=engine)



config.email
config.email = "johannes.ae@hotmail.de"
config.max_retries = 0
config.retry_backoff_factor = 0.1
config.retry_http_codes = [429, 500, 503]


def gl_journal_papers (journal_id) :
    
    # get pager to iterate on
    l_pager = Works().filter(
        primary_location= {"source": {"id" :journal_id}}).paginate(per_page=200, n_max = None)

    nbr_papers = Works().filter(primary_location= {"source": {"id" :journal_id}}).count()
    print(f"nbr_papers: {nbr_papers}")

    # download all the pages
    l_pages = []
    for page in l_pager:
        print(len(page))
        l_pages.append(page)
    
    # flatten to single articles
    l_papers = [x for xs in l_pages.copy() for x in xs]
    return(l_papers)

def gl_journal_info (concept_id):

    # breakpoint()

    l_pager = (Sources().filter(concept = {'id' : concept_id}, type= 'journal')
               .paginate(per_page = 200, n_max = None))

    nbr_journals = Sources().filter(concept = {'id' : concept_id}, type= 'journal').count()

    l_pages = [] 
    nbr_journals_dld = 0
    time_start = time.time()

    

    for page in l_pager:
        l_pages.append(page)
        
        time_dl = time.time()
        time_passed = round(time_dl - time_start,1)
        nbr_journals_dld += len(page)
        perc_dld = round(nbr_journals_dld*100/nbr_journals,1)
        print(f"{nbr_journals_dld}/{nbr_journals}({perc_dld}%) in {time_passed} secs")
        
    l_journals = [x for xs in l_pages.copy() for x in xs]
    return(l_journals)
    


def gc_ingest_cmd (entity, DIR_CSV):
    cmd = f"""clickhouse-client -d litanai --query "INSERT INTO {entity} FROM INFILE '{DIR_CSV}{entity}.csv.gz' COMPRESSION 'gzip' FORMAT CSV\""""

    return(cmd)


def pickle_entity (l_entities, entity_id, DIR_ENTITY_PICKLES):

    with open(os.path.join(DIR_ENTITY_PICKLES, entity_id), 'wb') as file:
        pickle.dump(l_entities, file)


def pickle_load_entity (entity_id, DIR_ENTITY_PICKLES):

    with open(os.path.join(DIR_ENTITY_PICKLES, entity_id), 'rb') as file:
        l_entities = pickle.load(file)

    return(l_entities)




def ingest_csv(DIR_CSV, l_entities) :
    "ingest the flattened files into clickhouse"
    # ch_client = clickhouse_connect.get_client(database = "litanai")

    

    l_cmd_ingest = [gc_ingest_cmd(entity, DIR_CSV) for entity in l_entities]
    # cmd_ingest_works = gc_ingest_cmd("works", DIR_CSV)
    # ch_client.command(cmd_ingest_works)

    
    # subprocess.run(cmd_ingest_works, shell = True, stdout=subprocess.PIPE, text = True)
    
    [subprocess.run(cmd_ingest, shell = True, stdout=subprocess.PIPE, text = True) for cmd_ingest in l_cmd_ingest]


    # class(dtx.iloc[314]['abstract_text'])
    # dtx.loc[313]


    # if False:
       # [os.remove(DIR_CSV + file) for file in os.listdir(DIR_CSV)]



def proc_journal (id_journal) :
    # download (or read), flatten and ingest a data

    id_journal_short = id_journal.replace('https://openalex.org/', '')
    print(f"id_journal_short: {id_journal_short}")
    
    # breakpoint()
    
    ## get data
    if id_journal_short not in os.listdir(DIR_JOURNAL_PICKLES):
        print("downloading papers")
        l_papers = gl_journal_papers(id_journal)
        pickle_entity(l_papers, id_journal_short, DIR_JOURNAL_PICKLES)
    else:
        l_papers = pickle_load_entity(id_journal_short, DIR_JOURNAL_PICKLES)
        print(f"retrieved {len(l_papers)} from file")
    
    print("flattening papers to csv")
    flatten_works(l_papers)
    
    # FIXME: ingestion should be conditional
    print("ingesting works")

    l_entities = ['works', 'works_related_works', 'works_referenced_works']
    ingest_csv(DIR_CSV, l_entities)


def proc_journal_info (id_concept) :
    id_concept_short = id_concept.replace('https://openalex.org/', '')
    print(f"id_journal_short: {id_concept_short}")

    # FIXME: proper paths
    if id_concept_short not in os.listdir(DIR_JOURNAL_PICKLES):
        print("downloading journal info")
        l_journals = gl_journal_info(id_concept)
        pickle_entity(l_journals)
    else:
        l_journals = pickle_load_entity(id_concept_short)
        print(f"retrieved {len(l_journals)} from file")

    print("flattening journal info to csv")
    flatten_sources(l_journals)

    print("ingesting journals")
    l_entities_journals = ["sources", "sources_counts_by_year", 'sources_ids']
    ingest_csv(DIR_CSV, l_entities_journals)

        

def get_very_related_works (l_seed_journals):
    "get related works, then get their journals later"

    # ch_client = clickhouse_connect.get_client(database = "litanai")

    # dt_relworks = ch_client.query_df("""
    # SELECT referenced_work_id, cnt FROM (  
    # SELECT referenced_work_id, COUNT(referenced_work_id) AS cnt
    # FROM works_related_works
    # GROUP BY referenced_work_id
    # ORDER BY cnt DESC
    # ) AS r ANTI LEFT JOIN works AS w ON r.referenced_work_id = w.id
    # limit 20""")

    # breakpoint()
    
    
    t_refworks = Table('works_referenced_works', metadata, autoload_with = engine)
    t_works = Table('works', metadata, autoload_with = engine)

    # qry = select(t_works.c.source_id, func.count(t_works.c.source_id)
    #        ).group_by(t_works.c.source_id)

    # pd.read_sql(qry, engine)
    
    # first subquery to get referenced work of e.g. poetics
    sq1 = (select(t_refworks.c.referenced_work_id.label("refw_id"),
                  func.count().label('cnt'))
           .where(t_works.c.source_id.in_(l_seed_journals))
           .join(t_refworks, t_works.c.id == t_refworks.c.work_id)
           .group_by(t_refworks.c.referenced_work_id)).subquery()

    # now filter out all those I have already
    sq2 = (select(sq1.c.refw_id, sq1.c.cnt).where(sq1.c.refw_id.notin_(select(t_works.c.id)))
           .order_by(desc('cnt'))).subquery()

    # print(sq1)
    # pd.read_sql(select(sq1), engine)
    
    
    # sq = select(t_refworks.c.referenced_work_id.label("refw_id"),
    #             func.count(t_refworks.c.referenced_work_id).label('cnt')                   
    #             ).group_by(t_refworks.c.referenced_work_id).order_by(desc('cnt')).subquery()
    # pd.read_sql(select(sq), engine)
    

    # use hacky query: not-in statement rather than anti-join
    # query_hacky = (select(sq.c.refw_id, sq.c.cnt)
    #                .where(sq.c.refw_id.notin_(select(t_works.c.id)))
    #                .limit(10))
     
    # query = (select(sq.c.referenced_work_id, sq.c.cnt)
    #          .outerjoin(t_works, sq.c.referenced_work_id == t_works.c.id))
    #          # .where(t_works.c.id.is_(None)))
    
    # print(query)
    # pd.read_sql(query_hacky, engine)
    # pd.read_sql(select(sq), engine)

    
    # dt_refworks = pd.read_sql(query_hacky, engine)
    dt_refworks = pd.read_sql(select(sq2).limit(20), engine)
    
    # https://openalex.org/W2084077463
    # breakpoint()
    
    l_works = Works()[list(dt_refworks['refw_id'])]
    # len(l_works)

    l_journals_prep = []
    for w in l_works:
        if prim_loc := w.get('primary_location'):
            if src := prim_loc.get('source'):
                if dispname := src.get('display_name'):
                    l_journals_prep.append(src)

    # get unique journals
    # l_journals = {j['id']:j for j in l_journals_prep}
    l_journals = list({j['id']:j for j in l_journals_prep}.values())

    # only get journals with less than 75k articles
    l_journals = [j for j in l_journals if Works().filter(primary_location=
                                                          {"source": {"id" :j['id']}}).count() < 200000]
    
    
    l_journalnames = [j['display_name'] for j in l_journals]
    print(l_journalnames)
    print(len(l_journalnames))

    return(l_journals)

# l_seed_journals = ['https://openalex.org/S31225034', 'https://openalex.org/s98355519',
#                    'https://openalex.org/s157620343']

    
# l_journals_to_dl = get_very_related_works(l_seed_journals)





proc_journal('https://openalex.org/C36289849')
