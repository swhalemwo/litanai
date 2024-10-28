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


uri = 'clickhouse://default:@localhost/litanai'
uri = 'clickhouse+native://localhost/litanai'
engine = create_engine(uri)
session = make_session(engine)
metadata = MetaData()

metadata.reflect(bind=engine)
# metadata.tables.keys()

DIR_CSV = '/home/johannes/Dropbox/proj/litanai/oa_csv_files/'
DIR_JOURNAL_PICKLES = "/run/media/johannes/data/litanai/journals/"




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

    

    # download all the pages
    l_pages = []
    for page in l_pager:
        print(len(page))
        l_pages.append(page)

    # flatten to single articles
    l_papers = [x for xs in l_pages.copy() for x in xs]
    return(l_papers)


def gc_ingest_cmd (entity, DIR_CSV):
    cmd = f"""clickhouse-client -d litanai --query "INSERT INTO {entity} FROM INFILE '{DIR_CSV}{entity}.csv.gz' COMPRESSION 'gzip' FORMAT CSV\""""

    return(cmd)


def pickle_journal (l_papers, journal_id, DIR_JOURNAL_PICKLES):

    with open(os.path.join(DIR_JOURNAL_PICKLES, journal_id), 'wb') as file:
        pickle.dump(l_papers, file)


def pickle_load_journal (journal_id, DIR_JOURNAL_PICKLES):

    with open(os.path.join(DIR_JOURNAL_PICKLES, journal_id), 'rb') as file:
        l_papers = pickle.load(file)

    return(l_papers)




def ingest_csv(DIR_CSV) :
    "ingest the flattened files into clickhouse"
    ch_client = clickhouse_connect.get_client(database = "litanai")

    
    l_entities = ['works', 'works_related_works', 'works_referenced_works']
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
        pickle_journal(l_papers, id_journal_short, DIR_JOURNAL_PICKLES)
    else:
        l_papers = pickle_load_journal(id_journal_short, DIR_JOURNAL_PICKLES)
        print(f"retrieved {len(l_papers)} from file")
    
    print("flattening papers to csv")
    flatten_works(l_papers)
    
    # FIXME: ingestion should be conditional
    print("ingesting works")
    ingest_csv(DIR_CSV)


def get_journals ():
    
    
    


DIR_CSV = '/home/johannes/Dropbox/proj/litanai/oa_csv_files/'
DIR_JOURNAL_PICKLES = "/run/media/johannes/data/litanai/journals/"



l_journals = ['https://openalex.org/s98355519', 'https://openalex.org/s157620343']  # poetics, ASR

[proc_journal(journal) for journal in l_journals]





Sources().count()

asr = Sources()["S157620343"]



Sources().filter(concept = {'id' : 'https://openalex.org/C144024400'}, type = "journal").count()



l_sources = Sources().filter(concept = {'id' : 'https://openalex.org/C144024400'}, type = "journal").get()

len(l_sources)
[print(i['display_name'], i['id']) for i in l_sources]



l_concepts = Concepts().search_filter(display_name = "sociology").get()
[print(i['display_name'], i['id']) for i in l_concepts]

l_topics = Topics().search_filter(display_name = "social sciences").get()
[print(i['display_name'], i['id']) for i in l_topics]


# Sources().filter(topics = {'id' : 'https://openalex.org/C144024400'}).count()
# topics doesn't work, have to use concept

Sources().filter(concept = {'id' : '3312'}).count()

# these work
Sources().filter(country_code = "US").count()
Sources().filter(is_oa = True).count() 
Sources().filter(type = "journal").count() 
Sources().filter(summary_stats = {'h_index' : 5}).count()

