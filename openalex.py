import pandas as pd
import pyalex
from pyalex import Works, Authors, Sources, Institutions, Topics, Publishers, Funders, config, invert_abstract

from flatten_openalex_jsonl import flatten_works, init_dict_writer

import clickhouse_connect
import pickle


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


def ingest_csv(DIR_CSV) :
    "ingest the flattened files into clickhouse"
    ch_client = clickhouse_connect.get_client(database = "litanai")

    cmd_ingest_works = gc_ingest_cmd("works", DIR_CSV)
    ch_client.command(cmd_ingest_works)

    subprocess.run(cmd_ingest_works, shell = True, stdout=subprocess.PIPE, text = True)
    

    # class(dtx.iloc[314]['abstract_text'])
    # dtx.loc[313]


    # if False:
    #     [os.remove(DIR_CSV + file) for file in os.listdir(DIR_CSV)]





DIR_JOURNAL_PICKLES = "/run/media/johannes/data/litanai/journals/"





l_papers_poetics = gl_journal_papers('https://openalex.org/S98355519')
flatten_works(l_papers_poetics)



l_papers_asr = gl_journal_papers('https://openalex.org/s157620343')
flatten_works(l_papers_asr)

pickle_journal(l_papers_asr, "s157620343", DIR_JOURNAL_PICKLES)




DIR_CSV = '/home/johannes/Dropbox/proj/litanai/oa_csv_files/'

ingest_csv(DIR_CSV)

nbr_papers = Works().filter(primary_location= {"source": {"id" :'https://openalex.org/s157620343'}}).count()
