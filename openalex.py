import csv
import glob
import gzip
import json
import os
import time
import pdb
import gc

import pandas as pd
import pyalex
from pyalex import Works, Authors, Sources, Institutions, Topics, Concepts, Publishers, Funders, config, invert_abstract

from flatten_openalex_jsonl import flatten_works, flatten_sources, init_dict_writer

from litanai import gd_bibtex


import clickhouse_connect
import pickle
import subprocess


from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, MetaData, create_engine, text, select, tuple_, func, desc, or_, and_, cast, Numeric, distinct
from clickhouse_sqlalchemy import Table, make_session, get_declarative_base, types, engines
from requests import Session


from globs import DIR_CSV, DIR_JOURNAL_PICKLES, DIR_JOURNAL_GZIP

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



def debugger_is_active() -> bool:
    """Return if the debugger is currently active"""
    return hasattr(sys, 'gettrace') and sys.gettrace() is not None


def lmap (fun, iterable):
    return (list(map(fun, iterable)))

def flatten_list (list_of_lists):
    return([x for xs in list_of_lists for x in xs])

def split_list (list_ts, max_sublist_len):
    return[list_ts[i:i + max_sublist_len] for i in range(0, len(list_ts), max_sublist_len)]


def convert_dld_file (id_short):

    gc.collect()
    print(id_short)

    with open(os.path.join(DIR_JOURNAL_PICKLES, id_short), 'rb') as file:
        l_entities = pickle.load(file)

    pickle_entity(l_entities, id_short, DIR_JOURNAL_GZIP)

    l_entities = 0
    gc.collect()
    


def dl_pages (pager, nbr_entities):
    "download pages of pager"

    l_pages = []
    time_start = time.time()
    nbr_entities_dld = 0 

    for page in pager:
    
        l_pages.append(page)
        time_dl = time.time()
        time_passed = round(time_dl - time_start,1)
        
        nbr_entities_dld += len(page)
        perc_dld = round(nbr_entities_dld*100/nbr_entities,1)
        print(f"{nbr_entities_dld}/{nbr_entities} ({perc_dld}%) in {time_passed} secs")

    # l_entities = [x for xs in l_pages.copy() for x in xs]
    l_entities = flatten_list(l_pages.copy())

    return(l_entities)



def gl_journal_longworks(journal_id, year):
    "download each year for a journal"

    
    # breakpoint()
    # get min and max year of articles
    nbr_works = Works().filter(primary_location= {"source": {"id" :journal_id}}, publication_year = year).count()
    print(nbr_works)
    if nbr_works == 0:
        l_longworks = []
    else: 
        pager = (Works().filter(primary_location= {"source": {"id" :journal_id}}, publication_year = year)
                 .paginate(per_page=200, n_max = None))
        
        l_longworks = dl_pages(pager, nbr_works)

    return(l_longworks)

def proc_journal_longworks (journal_id, switch_ingest):
    
    # breakpoint()
    id_journal_short = journal_id.replace('https://openalex.org/', '')
    print(f"id_journal_short: {id_journal_short}")


    year = 2024 # start with 2024, then count down
    
    year_skipped_in_row = 0

    # go through all the years (year_counts don't include all publications)
    while True:
        
        journal_year_id = f"{id_journal_short}_{year}"
        # if the year is not in the pickles, download it
        if journal_year_id + ".json.gz" not in os.listdir(DIR_JOURNAL_GZIP):
            print(f"downloading papers for {year}")
            
            l_longworks = gl_journal_longworks(journal_id, year)
            # len(l_longworks)
            
            pickle_entity(l_longworks, journal_year_id, DIR_JOURNAL_GZIP)
            b_data_fresh = True

        
        else:
            if switch_ingest == "always":
                # if year is downloaded, load it
                l_longworks = pickle_load_entity(journal_year_id, DIR_JOURNAL_GZIP)
            else:
                l_longworks = []
                
            print(f"retrieved {len(l_longworks)} from file")
            b_data_fresh = False

        l_entities_to_ingest = ['works', 'works_related_works', 'works_referenced_works']
        
        ingest_dispatcher(l_longworks, l_entities_to_ingest, switch_ingest, b_data_fresh)

        if len(l_longworks) == 0:
            year_skipped_in_row += 1
        else:
            year_skipped_in_row = 0

        if year_skipped_in_row == 10:
            break

        year -= 1

    


def gl_journal_works (journal_id) :
    

    nbr_works = Works().filter(primary_location= {"source": {"id" :journal_id}}).count()
    print(f"nbr_works: {nbr_works}")

    l_pager = Works().filter(
        primary_location= {"source": {"id" :journal_id}}).paginate(per_page=200, n_max = None)

    # download all the pages
    # l_pages = []
    # for page in l_pager:
    #     print(len(page))
    #     l_pages.append(page)

    if nbr_works == 0:
        l_works = []

    else:
        l_works = dl_pages(l_pager, nbr_works)

    return(l_works)

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

    # with open(os.path.join(DIR_ENTITY_PICKLES, entity_id), 'wb') as file:
    #     pickle.dump(l_entities, file)

    with gzip.open(os.path.join(DIR_ENTITY_PICKLES, entity_id) + ".json.gz", 'wt') as fx:
        json.dump(l_entities, fx)


def pickle_load_entity (entity_id, DIR_ENTITY_PICKLES):

    # with open(os.path.join(DIR_ENTITY_PICKLES, entity_id), 'rb') as file:
    #     l_entities = pickle.load(file)

    with gzip.open(os.path.join(DIR_ENTITY_PICKLES, entity_id) + '.json.gz', 'rt') as fx:
        l_entities = json.load(fx)

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


def ingest_dispatcher(l_entities, l_entities_to_ingest, switch_ingest, b_data_fresh):
    "flatten and ingest depending on switches"

    if switch_ingest == "only_fresh" and b_data_fresh == True:
        print("flattening papers to csv")
        flatten_works(l_entities)
    
        print("ingesting works")
        ingest_csv(DIR_CSV, l_entities_to_ingest)

    if switch_ingest == "only_fresh" and b_data_fresh == False:
        print("skip flattening and ingesting")
        # ingest_csv(DIR_CSV, l_entities_to_ingest)

    if switch_ingest == "always":
        
        print("flattening papers to csv")
        flatten_works(l_entities)
        print("ingesting works")
        ingest_csv(DIR_CSV, l_entities_to_ingest)

    if switch_ingest == 'never':
        print("skip flattening and ingesting")



def proc_journal_dispatch(journal_id, switch_ingest):
    "decide whether to download all works in one swoop or by year"

    nbr_papers = Works().filter(primary_location= {"source": {"id" :journal_id}}).count()

    if nbr_papers > 100000:
        proc_journal_longworks(journal_id, switch_ingest)
    else:
        proc_journal_works(journal_id, switch_ingest)
        


def proc_journal_works (id_journal, switch_ingest) :
    # download (or read), flatten and ingest a set of works from a journal

    # breakpoint()

    id_journal_short = id_journal.replace('https://openalex.org/', '')
    print(f"id_journal_short: {id_journal_short}")
    
        
    ## get data
    if id_journal_short + ".json.gz" not in os.listdir(DIR_JOURNAL_GZIP):
        print("downloading papers")
        l_papers = gl_journal_works(id_journal)
        pickle_entity(l_papers, id_journal_short, DIR_JOURNAL_GZIP)
        b_data_fresh = True
        
    else:
        # only load papers if they are to be ingested, else skip
        if switch_ingest == "always" :
            l_papers = pickle_load_entity(id_journal_short, DIR_JOURNAL_GZIP)
        else:
            l_papers = [] 
        print(f"retrieved {len(l_papers)} from file")
        b_data_fresh = False
    
    
    # FIXME: ingestion should be conditional
    
    l_entities_to_ingest = ['works', 'works_related_works', 'works_referenced_works']
    ingest_dispatcher(l_papers, l_entities_to_ingest, switch_ingest, b_data_fresh)
    
        

def proc_journal_info (id_concept) :

    # breakpoint() 

    id_concept_short = id_concept.replace('https://openalex.org/', '')
    print(f"id_journal_short: {id_concept_short}")

    # FIXME: proper paths
    if id_concept_short not in os.listdir(DIR_JOURNAL_GZIP):
        print("downloading journal info")
        l_journals = gl_journal_info(id_concept)
        pickle_entity(l_journals, id_concept_short, DIR_JOURNAL_GZIP)
    else:
        l_journals = pickle_load_entity(id_concept_short, DIR_JOURNAL_GZIP)
        print(f"retrieved {len(l_journals)} from file")

    print("flattening journal info to csv")
    flatten_sources(l_journals)

    # j = l_journals[18] # BJS
    # topics = j['topics']

    # [print(j['display_name']) for j in l_journals[0:40]]
    
    
    print("ingesting journals")
    l_entities_journals = ["sources", "sources_counts_by_year", 'sources_ids', 'sources_topics']
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
    # l_journals = [j for j in l_journals if Works().filter(primary_location=
    #                                                       {"source": {"id" :j['id']}}).count() < 200000]

    
    
    l_journalnames = [j['display_name'] for j in l_journals]
    print(l_journalnames)
    print(len(l_journalnames))

    return(l_journals)





# proc_journal('https://openalex.org/C36289849')

# l_concepts_to_dl = ["C36289849", "C144024400", "c17744445"]

# [proc_journal_info(c) for c in l_concepts_to_dl]

# proc_journal_info(l_concepts_to_dl[0])
# l_journal_info = gl_journal_info("C144024400")




# proc_journal_works(l_journals_to_dl[1], True)

# [proc_journal_dispatch(j, True) for j in l_journals_to_dl]


# proc_journal_longworks("https://openalex.org/S4210172589", True)

l_seed_journals = ['https://openalex.org/S31225034', 'https://openalex.org/s98355519',
                   'https://openalex.org/s157620343']

    
# l_journals_to_dl = get_very_related_works(l_seed_journals)

# [proc_journal_dispatch(j['id'], "only_fresh") for j in l_journals_to_dl]

# proc_journal_dispatch('https://openalex.org/S4306463937', "only_fresh")


l_journals_to_dl = [
    'https://openalex.org/S9692511',     'https://openalex.org/S78735424',    'https://openalex.org/S79135273'
    'https://openalex.org/S203860005',   'https://openalex.org/S19523265',    'https://openalex.org/S122767448'
    'https://openalex.org/S25746158',    'https://openalex.org/S50876694',    'https://openalex.org/S22535337'
    'https://openalex.org/S107859553',   'https://openalex.org/S195570583',   'https://openalex.org/S2764866340'
    'https://openalex.org/S203532909',   'https://openalex.org/S40975480',    'https://openalex.org/S206312523'
    'https://openalex.org/S4210205308',  'https://openalex.org/S3030097686',  'https://openalex.org/S4210189218'
    'https://openalex.org/S2764634626',  'https://openalex.org/S4210173017',  'https://openalex.org/S4210231835'
    'https://openalex.org/S4210238407',  'https://openalex.org/S4210213561',  'https://openalex.org/S4210218764'
    'https://openalex.org/S4210223487',  'https://openalex.org/S136211407',   'https://openalex.org/S173657377'
    'https://openalex.org/S91661715',    'https://openalex.org/S20010350',    'https://openalex.org/S172782825'
    'https://openalex.org/S160573970',   'https://openalex.org/S205875998',   'https://openalex.org/S59311786'
    'https://openalex.org/S41869786',    'https://openalex.org/S53787413',    'https://openalex.org/S14291815'
    'https://openalex.org/S114567169',   'https://openalex.org/S94663699',    'https://openalex.org/S4210194710'
    'https://openalex.org/S148909191',   'https://openalex.org/S53121397',    'https://openalex.org/S144836642'
    'https://openalex.org/S118409008',   'https://openalex.org/S65076878',    'https://openalex.org/S159612214'
    'https://openalex.org/S24152333',    'https://openalex.org/S4210172962',  'https://openalex.org/S4210168889'
    'https://openalex.org/S4210212285',  'https://openalex.org/S4210213693',  'https://openalex.org/S39307421'
    'https://openalex.org/S129389861',   'https://openalex.org/S97548893',    'https://openalex.org/S7424199'
    'https://openalex.org/S138645024',   'https://openalex.org/S1016481467',  'https://openalex.org/S71057445'
    'https://openalex.org/S2736490681',  'https://openalex.org/S138274780',   'https://openalex.org/S165087003'
    'https://openalex.org/S51211322',    'https://openalex.org/S146592948',   'https://openalex.org/S42468459'
    'https://openalex.org/S30699777',    'https://openalex.org/S24510439',    'https://openalex.org/S157001289'
    'https://openalex.org/S200414547',   'https://openalex.org/S175526339',   'https://openalex.org/S127589907'
    'https://openalex.org/S68754933',    'https://openalex.org/S202681640',   'https://openalex.org/S4210220442'
    'https://openalex.org/S4210180838',  'https://openalex.org/S4210215428',  'https://openalex.org/S35917800'
    'https://openalex.org/S98137347',    'https://openalex.org/S87933477',    'https://openalex.org/S187348256'
    'https://openalex.org/S61446109',    'https://openalex.org/S80967739',    'https://openalex.org/S118082279'
    'https://openalex.org/S94746221',    'https://openalex.org/S188481820',   'https://openalex.org/S187261251'
    'https://openalex.org/S96897161',    'https://openalex.org/S39973823',    'https://openalex.org/S115783906'
    'https://openalex.org/S70424273',    'https://openalex.org/S61715262',    'https://openalex.org/S2764570952'
    'https://openalex.org/S2737998943',  'https://openalex.org/S46039370',    'https://openalex.org/S35062419'
    'https://openalex.org/S2737545834',  'https://openalex.org/S98232940',    'https://openalex.org/S4210173062'
    'https://openalex.org/S4306534686',  'https://openalex.org/S4210194517',  'https://openalex.org/S4306501641'
    'https://openalex.org/S106822843',   'https://openalex.org/S165709033',   'https://openalex.org/S25370267'
    'https://openalex.org/S107737141',   'https://openalex.org/S40252047',    'https://openalex.org/S149872823'
    'https://openalex.org/S93630570',    'https://openalex.org/S38024979',    'https://openalex.org/S10082577'
    'https://openalex.org/S1131227',     'https://openalex.org/S179906392',   'https://openalex.org/S122290859'
    'https://openalex.org/S7345157',     'https://openalex.org/S149422333',   'https://openalex.org/S2898181366'
    'https://openalex.org/S8194976',     'https://openalex.org/S147910473',   'https://openalex.org/S31690342'
    'https://openalex.org/S15174096',    'https://openalex.org/S84764624',    'https://openalex.org/S2764471594'
    'https://openalex.org/S18869182',    'https://openalex.org/S2764961298',  'https://openalex.org/S4306513461'
    'https://openalex.org/S88020226',    'https://openalex.org/S166870025',   'https://openalex.org/S169586356'
    'https://openalex.org/S48690275',    'https://openalex.org/S129839026',   'https://openalex.org/S138679565'
    'https://openalex.org/S31064911',    'https://openalex.org/S120433428',   'https://openalex.org/S92608454'
    'https://openalex.org/S4210202845',  'https://openalex.org/S120387555',   'https://openalex.org/S46746144'
    'https://openalex.org/S119437531',   'https://openalex.org/S37813251',    'https://openalex.org/S107749620'
    'https://openalex.org/S183135890',   'https://openalex.org/S182336501',   'https://openalex.org/S143413576'
    'https://openalex.org/S27614628',    'https://openalex.org/S4210222380',  'https://openalex.org/S2764878774'
    'https://openalex.org/S2764769492',  'https://openalex.org/S4210200929',  'https://openalex.org/S2898150413'
    'https://openalex.org/S4210219630',  'https://openalex.org/S96544531',    'https://openalex.org/S2737558550'
    'https://openalex.org/S116184629',   'https://openalex.org/S63571384',    'https://openalex.org/S161743634'
    'https://openalex.org/S185874209',   'https://openalex.org/S154084123',   'https://openalex.org/S153485742'
    'https://openalex.org/S90670110',    'https://openalex.org/S25376279',    'https://openalex.org/S72915307'
    'https://openalex.org/S169433491',   'https://openalex.org/S158706580',   'https://openalex.org/S195167216'
    'https://openalex.org/S188819509',   'https://openalex.org/S2765021630',  'https://openalex.org/S8759915'
    'https://openalex.org/S4460519',     'https://openalex.org/S2765058493',  'https://openalex.org/S27614628'
    'https://openalex.org/S181171746',   'https://openalex.org/S4210202650',  'https://openalex.org/S4210186262'
    'https://openalex.org/S2738526030',  'https://openalex.org/S4306528578',  'https://openalex.org/S150314616'
    'https://openalex.org/S192650101',   'https://openalex.org/S27273401',    'https://openalex.org/S942037226'
    'https://openalex.org/S40137911',    'https://openalex.org/S28036099',    'https://openalex.org/S96563387'
    'https://openalex.org/S2764791026',  'https://openalex.org/S151119180',   'https://openalex.org/S733369'
    'https://openalex.org/S105066276',   'https://openalex.org/S17986141',    'https://openalex.org/S124362806'
    'https://openalex.org/S33125897',    'https://openalex.org/S104805611',   'https://openalex.org/S175590358'
    'https://openalex.org/S4210206464',  'https://openalex.org/S132495599',   'https://openalex.org/S135713760'
    'https://openalex.org/S83616544',    'https://openalex.org/S4210196933',  'https://openalex.org/S4306520822'
    'https://openalex.org/S4210240912',  'https://openalex.org/S23791984',    'https://openalex.org/S4306520823'
    'https://openalex.org/S8802318',     'https://openalex.org/S104917558',   'https://openalex.org/S87560609'
    'https://openalex.org/S13144211',    'https://openalex.org/S91660768',    'https://openalex.org/S64418186'
    'https://openalex.org/S9435936',     'https://openalex.org/S141988568',   'https://openalex.org/S88198767'
    'https://openalex.org/S207420867',   'https://openalex.org/S193412822',   'https://openalex.org/S176081437'
    'https://openalex.org/S179213746',   'https://openalex.org/S151944519',   'https://openalex.org/S43621796'
    'https://openalex.org/S80823180',    'https://openalex.org/S57027881',    'https://openalex.org/S36718530'
    'https://openalex.org/S2765035532',  'https://openalex.org/S146242404',   'https://openalex.org/S200275961'
    'https://openalex.org/S102275153',   'https://openalex.org/S158932249',   'https://openalex.org/S4210184327'
    'https://openalex.org/S4306520556',  'https://openalex.org/S102499938',   'https://openalex.org/S79054089'
    'https://openalex.org/S137832324',   'https://openalex.org/S146344',      'https://openalex.org/S191319304'
    'https://openalex.org/S89389284',    'https://openalex.org/S171183513',   'https://openalex.org/S187626162'
    'https://openalex.org/S66201313',    'https://openalex.org/S11392764',    'https://openalex.org/S12175909'
    'https://openalex.org/S17740374',    'https://openalex.org/S184239247',   'https://openalex.org/S199605113'
    'https://openalex.org/S90149737',    'https://openalex.org/S51001188',    'https://openalex.org/S2530642067'
    'https://openalex.org/S4210177192',  'https://openalex.org/S55323383',    'https://openalex.org/S4210230240'
    'https://openalex.org/S2764362400',  'https://openalex.org/S2496055428',  'https://openalex.org/S4210191868'
    'https://openalex.org/S9936406',     'https://openalex.org/S4210215240',  'https://openalex.org/S15470582'
    'https://openalex.org/S13389975',    'https://openalex.org/S50366009',    'https://openalex.org/S4210168021'
    'https://openalex.org/S28278612',    'https://openalex.org/S130455057',   'https://openalex.org/S199447588'
    'https://openalex.org/S168863142',   'https://openalex.org/S135297974',   'https://openalex.org/S71968408'
    'https://openalex.org/S147692640',   'https://openalex.org/S94236332',    'https://openalex.org/S11810700'
    'https://openalex.org/S100176667',   'https://openalex.org/S27211427',    'https://openalex.org/S106098198'
    'https://openalex.org/S51698217',    'https://openalex.org/S2764742558',  'https://openalex.org/S2765018649'
    'https://openalex.org/S87168931',    'https://openalex.org/S2764571748',  'https://openalex.org/S2739293849'
    'https://openalex.org/S197465442',   'https://openalex.org/S2764845239',  'https://openalex.org/S52417371'
    'https://openalex.org/S86033158',    'https://openalex.org/S9731383',     'https://openalex.org/S64744539'
    'https://openalex.org/S141724154',   'https://openalex.org/S62009534',    'https://openalex.org/S22283869'
    'https://openalex.org/S190942573',   'https://openalex.org/S4210208415',  'https://openalex.org/S4210188777'
    'https://openalex.org/S55099511',    'https://openalex.org/S4210183760',  'https://openalex.org/S4210193178'
    'https://openalex.org/S2735686177']

# [proc_journal_dispatch(j, "always") for j in l_journals_to_dl[0:2]]
# [proc_journal_dispatch(j, "only_fresh") for j in l_journals_to_dl]

# files_to_cvrt = os.listdir(DIR_JOURNAL_PICKLES)
# lmap(convert_dld_file, files_to_cvrt[0:5])




