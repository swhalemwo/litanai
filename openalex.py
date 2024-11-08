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
    'https://openalex.org/S187270080', 'https://openalex.org/S83406069', 'https://openalex.org/S132285554',
    'https://openalex.org/S2764871768', 'https://openalex.org/S105812952', 'https://openalex.org/S165351902',
    'https://openalex.org/S191556934', 'https://openalex.org/S149131268', 'https://openalex.org/S128779766',
    'https://openalex.org/S4210169622', 'https://openalex.org/S158923391', 'https://openalex.org/S67103694',
    'https://openalex.org/S177779441', 'https://openalex.org/S88020226', 'https://openalex.org/S63576417',
    'https://openalex.org/S2738076829', 'https://openalex.org/S8124760', 'https://openalex.org/S176795851',
    'https://openalex.org/S111964278', 'https://openalex.org/S192723185', 'https://openalex.org/S3416249',
    'https://openalex.org/S102140811', 'https://openalex.org/S199447588', 'https://openalex.org/S186255157',
    'https://openalex.org/S77308338', 'https://openalex.org/S4210203252', 'https://openalex.org/S16035445',
    'https://openalex.org/S92260147', 'https://openalex.org/S158705510', 'https://openalex.org/S50561683',
    'https://openalex.org/S139232146', 'https://openalex.org/S94044085', 'https://openalex.org/S121852782',
    'https://openalex.org/S149146142', 'https://openalex.org/S125977778', 'https://openalex.org/S187788904',
    'https://openalex.org/S179575189', 'https://openalex.org/S126013225', 'https://openalex.org/S201186940',
    'https://openalex.org/S58438227', 'https://openalex.org/S140218963', 'https://openalex.org/S48254654',
    'https://openalex.org/S171971387', 'https://openalex.org/S4210185306', 'https://openalex.org/S3006283864',
    'https://openalex.org/S39541053', 'https://openalex.org/S144402922', 'https://openalex.org/S102624508',
    'https://openalex.org/S55766468', 'https://openalex.org/S18727733', 'https://openalex.org/S52721111',
    'https://openalex.org/S202934171', 'https://openalex.org/S43637407', 'https://openalex.org/S61028752',
    'https://openalex.org/S66666449', 'https://openalex.org/S187200255', 'https://openalex.org/S148277943',
    'https://openalex.org/S142306484', 'https://openalex.org/S144747835', 'https://openalex.org/S9731383',
    'https://openalex.org/S22422391', 'https://openalex.org/S163134310', 'https://openalex.org/S4210189519',
    'https://openalex.org/S2764516016', 'https://openalex.org/S158332939', 'https://openalex.org/S73388012',
    'https://openalex.org/S53726244', 'https://openalex.org/S61446109', 'https://openalex.org/S156003414',
    'https://openalex.org/S171234518', 'https://openalex.org/S49321836', 'https://openalex.org/S135297974',
    'https://openalex.org/S187013691', 'https://openalex.org/S119514724', 'https://openalex.org/S31748134',
    'https://openalex.org/S195776158', 'https://openalex.org/S55433255', 'https://openalex.org/S82593474',
    'https://openalex.org/S4306500801', 'https://openalex.org/S174105208', 'https://openalex.org/S175759345',
    'https://openalex.org/S2764380667', 'https://openalex.org/S68503284', 'https://openalex.org/S4210189644',
    'https://openalex.org/S37738278', 'https://openalex.org/S25059752', 'https://openalex.org/S4210228947',
    'https://openalex.org/S111656376', 'https://openalex.org/S129906632', 'https://openalex.org/S41958068',
    'https://openalex.org/S4210204369', 'https://openalex.org/S187626162', 'https://openalex.org/S105581714',
    'https://openalex.org/S1013860869', 'https://openalex.org/S4210230633', 'https://openalex.org/S149709236',
    'https://openalex.org/S133059572', 'https://openalex.org/S165789823', 'https://openalex.org/S49549746',
    'https://openalex.org/S117460449', 'https://openalex.org/S37320504', 'https://openalex.org/S191031105',
    'https://openalex.org/S4210228753', 'https://openalex.org/S71968408', 'https://openalex.org/S4210172426',
    'https://openalex.org/S189466069', 'https://openalex.org/S132210109', 'https://openalex.org/S2764440460',
    'https://openalex.org/S161695446', 'https://openalex.org/S70175541', 'https://openalex.org/S19400212',
    'https://openalex.org/S2764816500', 'https://openalex.org/S2764791026', 'https://openalex.org/S203071086',
    'https://openalex.org/S201037040', 'https://openalex.org/S99768379', 'https://openalex.org/S129389861',
    'https://openalex.org/S101378441', 'https://openalex.org/S90998793', 'https://openalex.org/S10496491',
    'https://openalex.org/S90678448', 'https://openalex.org/S48492747', 'https://openalex.org/S84268765',
    'https://openalex.org/S11782782', 'https://openalex.org/S87328381', 'https://openalex.org/S8557221',
    'https://openalex.org/S74338687', 'https://openalex.org/S203746597', 'https://openalex.org/S149486094',
    'https://openalex.org/S161053114', 'https://openalex.org/S47993699', 'https://openalex.org/S2764810659',
    'https://openalex.org/S166797808', 'https://openalex.org/S18284184', 'https://openalex.org/S203860005',
    'https://openalex.org/S5352200', 'https://openalex.org/S88978795', 'https://openalex.org/S169221615',
    'https://openalex.org/S143806524', 'https://openalex.org/S136009100', 'https://openalex.org/S196593774',
    'https://openalex.org/S157600745', 'https://openalex.org/S2734879500', 'https://openalex.org/S7345157',
    'https://openalex.org/S103253184', 'https://openalex.org/S90670110', 'https://openalex.org/S193408319',
    'https://openalex.org/S4210215964', 'https://openalex.org/S118000741', 'https://openalex.org/S174538591',
    'https://openalex.org/S4210174680', 'https://openalex.org/S185082372', 'https://openalex.org/S2764925558',
    'https://openalex.org/S45682993', 'https://openalex.org/S110036823', 'https://openalex.org/S2764438959',
    'https://openalex.org/S200274427', 'https://openalex.org/S49331332', 'https://openalex.org/S114598798',
    'https://openalex.org/S26544713', 'https://openalex.org/S53056752', 'https://openalex.org/S579031',
    'https://openalex.org/S75832472', 'https://openalex.org/S38696330', 'https://openalex.org/S42028863',
    'https://openalex.org/S35090897', 'https://openalex.org/S143995394', 'https://openalex.org/S4210199364',
    'https://openalex.org/S2764772869', 'https://openalex.org/S3835251', 'https://openalex.org/S72844074',
    'https://openalex.org/S49396926', 'https://openalex.org/S4210239688', 'https://openalex.org/S2765043386',
    'https://openalex.org/S184239247', 'https://openalex.org/S64831726', 'https://openalex.org/S4210200126',
    'https://openalex.org/S61992289', 'https://openalex.org/S110667073', 'https://openalex.org/S323237',
    'https://openalex.org/S123043354', 'https://openalex.org/S2764459744', 'https://openalex.org/S36178057',
    'https://openalex.org/S5043877', 'https://openalex.org/S207068962', 'https://openalex.org/S172342871',
    'https://openalex.org/S2798341630', 'https://openalex.org/S2596068225', 'https://openalex.org/S38942948',
    'https://openalex.org/S165865588', 'https://openalex.org/S120253869', 'https://openalex.org/S51144326',
    'https://openalex.org/S82530051', 'https://openalex.org/S180941013', 'https://openalex.org/S1014724869',
    'https://openalex.org/S160506855', 'https://openalex.org/S4210224032', 'https://openalex.org/S95636294',
    'https://openalex.org/S83616544', 'https://openalex.org/S201221823', 'https://openalex.org/S100134864',
    'https://openalex.org/S207100277', 'https://openalex.org/S113497174', 'https://openalex.org/S128223704',
    'https://openalex.org/S54899181', 'https://openalex.org/S162068045', 'https://openalex.org/S201928713',
    'https://openalex.org/S18053921', 'https://openalex.org/S57293258', 'https://openalex.org/S152897404',
    'https://openalex.org/S171720814', 'https://openalex.org/S97919111', 'https://openalex.org/S58891722',
    'https://openalex.org/S41869786', 'https://openalex.org/S4210230413', 'https://openalex.org/S27428843',
    'https://openalex.org/S4306505307', 'https://openalex.org/S89717723', 'https://openalex.org/S25359449',
    'https://openalex.org/S27211427', 'https://openalex.org/S134979503', 'https://openalex.org/S108875122',
    'https://openalex.org/S138645024', 'https://openalex.org/S116345831', 'https://openalex.org/S207416075',
    'https://openalex.org/S13968111', 'https://openalex.org/S133599136', 'https://openalex.org/S60370010',
    'https://openalex.org/S170137484', 'https://openalex.org/S82455696', 'https://openalex.org/S148397273',
    'https://openalex.org/S158451771', 'https://openalex.org/S2764837548', 'https://openalex.org/S148068116',
    'https://openalex.org/S4210206302', 'https://openalex.org/S103321696', 'https://openalex.org/S164302473',
    'https://openalex.org/S37739784', 'https://openalex.org/S180784258', 'https://openalex.org/S49080740',
    'https://openalex.org/S73612404', 'https://openalex.org/S110151151', 'https://openalex.org/S196762031',
    'https://openalex.org/S148090250', 'https://openalex.org/S175672440', 'https://openalex.org/S900338',
    'https://openalex.org/S88884646', 'https://openalex.org/S27354837', 'https://openalex.org/S98984247',
    'https://openalex.org/S4210183922', 'https://openalex.org/S2735890421', 'https://openalex.org/S118637542',
    'https://openalex.org/S2502551423', 'https://openalex.org/S945769174', 'https://openalex.org/S2764989655',
    'https://openalex.org/S155657779', 'https://openalex.org/S2764726166', 'https://openalex.org/S52048951',
    'https://openalex.org/S2764672113', 'https://openalex.org/S2764393121', 'https://openalex.org/S140564781',
    'https://openalex.org/S6959732', 'https://openalex.org/S147265922', 'https://openalex.org/S4210190151',
    'https://openalex.org/S2764486912', 'https://openalex.org/S147692640', 'https://openalex.org/S42893225',
    'https://openalex.org/S168863142', 'https://openalex.org/S65353606', 'https://openalex.org/S175013504',
    'https://openalex.org/S203585231', 'https://openalex.org/S184446198', 'https://openalex.org/S117214846',
    'https://openalex.org/S205793605', 'https://openalex.org/S197939330', 'https://openalex.org/S92522684',
    'https://openalex.org/S138400101', 'https://openalex.org/S95454600', 'https://openalex.org/S161350934',
    'https://openalex.org/S2737866954', 'https://openalex.org/S145099083', 'https://openalex.org/S89389284',
    'https://openalex.org/S134953813', 'https://openalex.org/S189751245', 'https://openalex.org/S4210202845',
    'https://openalex.org/S146984626', 'https://openalex.org/S23343376', 'https://openalex.org/S67638930',
    'https://openalex.org/S65924262', 'https://openalex.org/S44090634', 'https://openalex.org/S108453729',
    'https://openalex.org/S76877748', 'https://openalex.org/S162526994', 'https://openalex.org/S79946630',
    'https://openalex.org/S41930762', 'https://openalex.org/S4306499090', 'https://openalex.org/S133913429',
    'https://openalex.org/S154010748', 'https://openalex.org/S128478731', 'https://openalex.org/S179028992',
    'https://openalex.org/S204305389', 'https://openalex.org/S12410666', 'https://openalex.org/S135539873',
    'https://openalex.org/S34542846', 'https://openalex.org/S37376905', 'https://openalex.org/S66201313',
    'https://openalex.org/S36209479', 'https://openalex.org/S2898340276', 'https://openalex.org/S4306533116',
    'https://openalex.org/S178021067', 'https://openalex.org/S23753256', 'https://openalex.org/S4210206464',
    'https://openalex.org/S2764593888', 'https://openalex.org/S73786128', 'https://openalex.org/S90314269',
    'https://openalex.org/S84664706', 'https://openalex.org/S143982487', 'https://openalex.org/S35673206',
    'https://openalex.org/S4888523', 'https://openalex.org/S4210197678', 'https://openalex.org/S26073974',
    'https://openalex.org/S93133213', 'https://openalex.org/S119908498', 'https://openalex.org/S114567169',
    'https://openalex.org/S34649780', 'https://openalex.org/S4306535235', 'https://openalex.org/S47864991',
    'https://openalex.org/S103311983', 'https://openalex.org/S18354546', 'https://openalex.org/S128621090',
    'https://openalex.org/S190691735', 'https://openalex.org/S15765371', 'https://openalex.org/S26608901',
    'https://openalex.org/S4210230847', 'https://openalex.org/S121651287', 'https://openalex.org/S159120381',
    'https://openalex.org/S96919139', 'https://openalex.org/S167809449', 'https://openalex.org/S157779219',
    'https://openalex.org/S25720340', 'https://openalex.org/S21668869', 'https://openalex.org/S191871414',
    'https://openalex.org/S12175909', 'https://openalex.org/S4210229212', 'https://openalex.org/S957286808',
    'https://openalex.org/S45217375', 'https://openalex.org/S2764854513', 'https://openalex.org/S100167944',
    'https://openalex.org/S115237062', 'https://openalex.org/S90873577', 'https://openalex.org/S2764989054',
    'https://openalex.org/S129086836', 'https://openalex.org/S90283396', 'https://openalex.org/S4210167948',
    'https://openalex.org/S51885707', 'https://openalex.org/S4210210868', 'https://openalex.org/S18318442',
    'https://openalex.org/S200838492', 'https://openalex.org/S2764807727', 'https://openalex.org/S2764908727',
    'https://openalex.org/S149064342', 'https://openalex.org/S4210206495', 'https://openalex.org/S4210186007',
    'https://openalex.org/S2765038058', 'https://openalex.org/S82316188', 'https://openalex.org/S4210188312',
    'https://openalex.org/S161221444', 'https://openalex.org/S170810187', 'https://openalex.org/S114365116',
    'https://openalex.org/S121026525', 'https://openalex.org/S4210189271', 'https://openalex.org/S4306530378',
    'https://openalex.org/S44376543', 'https://openalex.org/S29817978', 'https://openalex.org/S106702950',
    'https://openalex.org/S93630570', 'https://openalex.org/S42023019', 'https://openalex.org/S9856456',
    'https://openalex.org/S118812498', 'https://openalex.org/S176497116', 'https://openalex.org/S4306505481',
    'https://openalex.org/S51277951', 'https://openalex.org/S69799158', 'https://openalex.org/S185256354',
    'https://openalex.org/S173807628', 'https://openalex.org/S146918421', 'https://openalex.org/S2764660481',
    'https://openalex.org/S2764746430', 'https://openalex.org/S193575162', 'https://openalex.org/S202812398',
    'https://openalex.org/S103858575', 'https://openalex.org/S96839988', 'https://openalex.org/S47098207',
    'https://openalex.org/S105044966', 'https://openalex.org/S188819509', 'https://openalex.org/S71218591',
    'https://openalex.org/S112952035', 'https://openalex.org/S84394799', 'https://openalex.org/S76633192',
    'https://openalex.org/S119437531', 'https://openalex.org/S135158421', 'https://openalex.org/S190363454',
    'https://openalex.org/S80823180', 'https://openalex.org/S4210232956', 'https://openalex.org/S139478629',
    'https://openalex.org/S179213746', 'https://openalex.org/S5195456', 'https://openalex.org/S4210233753',
    'https://openalex.org/S4210194518', 'https://openalex.org/S2738355984', 'https://openalex.org/S150535073',
    'https://openalex.org/S149070780', 'https://openalex.org/S32136629', 'https://openalex.org/S202512192',
    'https://openalex.org/S189738091', 'https://openalex.org/S72648116', 'https://openalex.org/S94775995',
    'https://openalex.org/S2764703659', 'https://openalex.org/S105556297', 'https://openalex.org/S2764542025',
    'https://openalex.org/S27632631', 'https://openalex.org/S201645574', 'https://openalex.org/S160393188',
    'https://openalex.org/S29659387', 'https://openalex.org/S177092143', 'https://openalex.org/S2764708357',
    'https://openalex.org/S97033246', 'https://openalex.org/S172501620', 'https://openalex.org/S2764382203',
    'https://openalex.org/S120518121', 'https://openalex.org/S4306532704', 'https://openalex.org/S164321952',
    'https://openalex.org/S149205526', 'https://openalex.org/S192056284', 'https://openalex.org/S181388337',
    'https://openalex.org/S173589923', 'https://openalex.org/S201675752', 'https://openalex.org/S77556226',
    'https://openalex.org/S170913539', 'https://openalex.org/S88935262', 'https://openalex.org/S142131192',
    'https://openalex.org/S30543418', 'https://openalex.org/S31120751', 'https://openalex.org/S4210197218',
    'https://openalex.org/S206927904', 'https://openalex.org/S202178085', 'https://openalex.org/S2764635653',
    'https://openalex.org/S50631486', 'https://openalex.org/S6596815', 'https://openalex.org/S163545350',
    'https://openalex.org/S32670438', 'https://openalex.org/S152304618', 'https://openalex.org/S42678134',
    'https://openalex.org/S2738281210', 'https://openalex.org/S154433558', 'https://openalex.org/S3022066471',
    'https://openalex.org/S95691132', 'https://openalex.org/S12818680', 'https://openalex.org/S4210224324',
    'https://openalex.org/S2736813639', 'https://openalex.org/S6814119', 'https://openalex.org/S975761300',
    'https://openalex.org/S201345032', 'https://openalex.org/S140059072', 'https://openalex.org/S22247924',
    'https://openalex.org/S95650557', 'https://openalex.org/S39476178', 'https://openalex.org/S184793761',
    'https://openalex.org/S47042909', 'https://openalex.org/S27931387', 'https://openalex.org/S184846731',
    'https://openalex.org/S158647270', 'https://openalex.org/S135892541', 'https://openalex.org/S60510972',
    'https://openalex.org/S2764641372', 'https://openalex.org/S111116695', 'https://openalex.org/S76440981',
    'https://openalex.org/S4306521824', 'https://openalex.org/S25593863', 'https://openalex.org/S170231156',
    'https://openalex.org/S2047265', 'https://openalex.org/S2764685317', 'https://openalex.org/S36547454',
    'https://openalex.org/S135086714', 'https://openalex.org/S203532909', 'https://openalex.org/S172313144',
    'https://openalex.org/S161221442', 'https://openalex.org/S4210206027', 'https://openalex.org/S192554527',
    'https://openalex.org/S9435936', 'https://openalex.org/S89594170', 'https://openalex.org/S782382',
    'https://openalex.org/S154533451', 'https://openalex.org/S17740374', 'https://openalex.org/S66524056',
    'https://openalex.org/S62110574', 'https://openalex.org/S9702615', 'https://openalex.org/S180126918',
    'https://openalex.org/S30879505', 'https://openalex.org/S998097505', 'https://openalex.org/S2765053757',
    'https://openalex.org/S39182069', 'https://openalex.org/S19927788', 'https://openalex.org/S107325288',
    'https://openalex.org/S91661715', 'https://openalex.org/S198892436', 'https://openalex.org/S117414780',
    'https://openalex.org/S12780936', 'https://openalex.org/S114020440', 'https://openalex.org/S141683002',
    'https://openalex.org/S4138479', 'https://openalex.org/S4210228916', 'https://openalex.org/S174197836',
    'https://openalex.org/S184126201', 'https://openalex.org/S4210168091', 'https://openalex.org/S30594836',
    'https://openalex.org/S162232022', 'https://openalex.org/S190460170', 'https://openalex.org/S201731998',
    'https://openalex.org/S2764345705', 'https://openalex.org/S967494592', 'https://openalex.org/S16390937',
    'https://openalex.org/S61413993', 'https://openalex.org/S108349823', 'https://openalex.org/S118198609',
    'https://openalex.org/S136661304', 'https://openalex.org/S189112142', 'https://openalex.org/S43587112',
    'https://openalex.org/S96072387', 'https://openalex.org/S106822843', 'https://openalex.org/S176150786',
    'https://openalex.org/S96897161', 'https://openalex.org/S18835586', 'https://openalex.org/S164338815',
    'https://openalex.org/S195553305', 'https://openalex.org/S134363990', 'https://openalex.org/S145200904',
    'https://openalex.org/S130179515', 'https://openalex.org/S81125728', 'https://openalex.org/S168717511',
    'https://openalex.org/S19534303', 'https://openalex.org/S100014455', 'https://openalex.org/S961603',
    'https://openalex.org/S70698675', 'https://openalex.org/S78792899', 'https://openalex.org/S2737251451',
    'https://openalex.org/S2764943951', 'https://openalex.org/S114140714', 'https://openalex.org/S4210171597',
    'https://openalex.org/S4210184645', 'https://openalex.org/S4210167491', 'https://openalex.org/S4210172902',
    'https://openalex.org/S43415676', 'https://openalex.org/S4210222123', 'https://openalex.org/S2764449526',
    'https://openalex.org/S91956071', 'https://openalex.org/S163774179', 'https://openalex.org/S181493553',
    'https://openalex.org/S2764627845', 'https://openalex.org/S163221538', 'https://openalex.org/S148909191',
    'https://openalex.org/S177792750', 'https://openalex.org/S102896891', 'https://openalex.org/S136339162',
    'https://openalex.org/S2739018597', 'https://openalex.org/S4306535176', 'https://openalex.org/S2738933148',
    'https://openalex.org/S124584455', 'https://openalex.org/S166002381', 'https://openalex.org/S35298862',
    'https://openalex.org/S55074384', 'https://openalex.org/S93028289', 'https://openalex.org/S96006530',
    'https://openalex.org/S14568501', 'https://openalex.org/S2736490681', 'https://openalex.org/S1558358',
    'https://openalex.org/S189354248', 'https://openalex.org/S32314625', 'https://openalex.org/S4306534810',
    'https://openalex.org/S157344451', 'https://openalex.org/S156260212', 'https://openalex.org/S185905316',
    'https://openalex.org/S4210213389', 'https://openalex.org/S960608072', 'https://openalex.org/S10288104',
    'https://openalex.org/S2764460092', 'https://openalex.org/S159675017', 'https://openalex.org/S4210226649',
    'https://openalex.org/S6013065', 'https://openalex.org/S3354691', 'https://openalex.org/S193228710',
    'https://openalex.org/S192660949', 'https://openalex.org/S166621295', 'https://openalex.org/S190942573',
    'https://openalex.org/S51211322', 'https://openalex.org/S2764498099', 'https://openalex.org/S153570774',
    'https://openalex.org/S183232449', 'https://openalex.org/S116712389', 'https://openalex.org/S51184730',
    'https://openalex.org/S113116626', 'https://openalex.org/S192266589', 'https://openalex.org/S2764631557',
    'https://openalex.org/S81410195', 'https://openalex.org/S134094273', 'https://openalex.org/S4210215475',
    'https://openalex.org/S90857023', 'https://openalex.org/S93284759', 'https://openalex.org/S191693275',
    'https://openalex.org/S197396629', 'https://openalex.org/S204225869', 'https://openalex.org/S2764860561',
    'https://openalex.org/S156995526', 'https://openalex.org/S64744539', 'https://openalex.org/S4306525247',
    'https://openalex.org/S14764538', 'https://openalex.org/S132310603', 'https://openalex.org/S169433491',
    'https://openalex.org/S185253248', 'https://openalex.org/S5465880', 'https://openalex.org/S130699561',
    'https://openalex.org/S173789977', 'https://openalex.org/S29411750', 'https://openalex.org/S61595665',
    'https://openalex.org/S145471836', 'https://openalex.org/S93195727', 'https://openalex.org/S79007945',
    'https://openalex.org/S26862755', 'https://openalex.org/S267729', 'https://openalex.org/S25235501',
    'https://openalex.org/S50174339', 'https://openalex.org/S57522439', 'https://openalex.org/S107033909',
    'https://openalex.org/S180851279', 'https://openalex.org/S124549413', 'https://openalex.org/S119587767',
    'https://openalex.org/S107737141', 'https://openalex.org/S52396224', 'https://openalex.org/S184583347',
    'https://openalex.org/S968139600', 'https://openalex.org/S126499095', 'https://openalex.org/S20152851',
    'https://openalex.org/S123914889', 'https://openalex.org/S184816971', 'https://openalex.org/S4210239474',
    'https://openalex.org/S195297104', 'https://openalex.org/S9475327', 'https://openalex.org/S141901250',
    'https://openalex.org/S2764419184', 'https://openalex.org/S199690760', 'https://openalex.org/S49131624',
    'https://openalex.org/S2764927701', 'https://openalex.org/S2764481192', 'https://openalex.org/S12430390',
    'https://openalex.org/S4210173133', 'https://openalex.org/S138235560', 'https://openalex.org/S2911660974',
    'https://openalex.org/S132538520', 'https://openalex.org/S4210174900', 'https://openalex.org/S1390998',
    'https://openalex.org/S140455145', 'https://openalex.org/S147670119', 'https://openalex.org/S2764834030',
    'https://openalex.org/S71487352', 'https://openalex.org/S13823553', 'https://openalex.org/S115626870',
    'https://openalex.org/S88603889', 'https://openalex.org/S62957338', 'https://openalex.org/S100176667',
    'https://openalex.org/S107036887', 'https://openalex.org/S118409008', 'https://openalex.org/S137473582',
    'https://openalex.org/S202734349', 'https://openalex.org/S2765035532', 'https://openalex.org/S82178847',
    'https://openalex.org/S123822488', 'https://openalex.org/S1234856', 'https://openalex.org/S4210190227',
    'https://openalex.org/S113237667', 'https://openalex.org/S173776643', 'https://openalex.org/S121204338',
    'https://openalex.org/S187239619', 'https://openalex.org/S4210195760', 'https://openalex.org/S89175547',
    'https://openalex.org/S2764850994', 'https://openalex.org/S27228949', 'https://openalex.org/S176667235',
    'https://openalex.org/S159137118', 'https://openalex.org/S17167983', 'https://openalex.org/S9954729',
    'https://openalex.org/S114339905', 'https://openalex.org/S33741590', 'https://openalex.org/S107631327',
    'https://openalex.org/S19358283', 'https://openalex.org/S2764759447', 'https://openalex.org/S4210225646',
    'https://openalex.org/S11810700', 'https://openalex.org/S163499366', 'https://openalex.org/S2764598273',
    'https://openalex.org/S4210192138', 'https://openalex.org/S2737582874', 'https://openalex.org/S4306521936',
    'https://openalex.org/S197145128', 'https://openalex.org/S55835182', 'https://openalex.org/S107859553',
    'https://openalex.org/S148413113', 'https://openalex.org/S57667410', 'https://openalex.org/S68758695',
    'https://openalex.org/S174413665', 'https://openalex.org/S90149737', 'https://openalex.org/S25746158',
    'https://openalex.org/S4210197299', 'https://openalex.org/S184351885', 'https://openalex.org/S105567970',
    'https://openalex.org/S2738683535', 'https://openalex.org/S62201805', 'https://openalex.org/S160573970',
    'https://openalex.org/S20082545', 'https://openalex.org/S38883057', 'https://openalex.org/S123955271',
    'https://openalex.org/S171424506', 'https://openalex.org/S66510378', 'https://openalex.org/S28604305',
    'https://openalex.org/S88763227', 'https://openalex.org/S161552967', 'https://openalex.org/S58171247',
    'https://openalex.org/S168682784', 'https://openalex.org/S81071032', 'https://openalex.org/S182323184',
    'https://openalex.org/S2913204857', 'https://openalex.org/S165253824', 'https://openalex.org/S152282257',
    'https://openalex.org/S4210168464', 'https://openalex.org/S26978096', 'https://openalex.org/S176330965',
    'https://openalex.org/S148599315', 'https://openalex.org/S179979277', 'https://openalex.org/S4210183012',
    'https://openalex.org/S61066606', 'https://openalex.org/S204030396', 'https://openalex.org/S153849593',
    'https://openalex.org/S9692511',     'https://openalex.org/S78735424',    'https://openalex.org/S79135273',
    'https://openalex.org/S203860005',   'https://openalex.org/S19523265',    'https://openalex.org/S122767448',
    'https://openalex.org/S25746158',    'https://openalex.org/S50876694',    'https://openalex.org/S22535337',
    'https://openalex.org/S107859553',   'https://openalex.org/S195570583',   'https://openalex.org/S2764866340',
    'https://openalex.org/S203532909',   'https://openalex.org/S40975480',    'https://openalex.org/S206312523',
    'https://openalex.org/S4210205308',  'https://openalex.org/S3030097686',  'https://openalex.org/S4210189218',
    'https://openalex.org/S2764634626',  'https://openalex.org/S4210173017',  'https://openalex.org/S4210231835',
    'https://openalex.org/S4210238407',  'https://openalex.org/S4210213561',  'https://openalex.org/S4210218764',
    'https://openalex.org/S4210223487',  'https://openalex.org/S136211407',   'https://openalex.org/S173657377',
    'https://openalex.org/S91661715',    'https://openalex.org/S20010350',    'https://openalex.org/S172782825',
    'https://openalex.org/S160573970',   'https://openalex.org/S205875998',   'https://openalex.org/S59311786',
    'https://openalex.org/S41869786',    'https://openalex.org/S53787413',    'https://openalex.org/S14291815',
    'https://openalex.org/S114567169',   'https://openalex.org/S94663699',    'https://openalex.org/S4210194710',
    'https://openalex.org/S148909191',   'https://openalex.org/S53121397',    'https://openalex.org/S144836642',
    'https://openalex.org/S118409008',   'https://openalex.org/S65076878',    'https://openalex.org/S159612214',
    'https://openalex.org/S24152333',    'https://openalex.org/S4210172962',  'https://openalex.org/S4210168889',
    'https://openalex.org/S4210212285',  'https://openalex.org/S4210213693',  'https://openalex.org/S39307421',
    'https://openalex.org/S129389861',   'https://openalex.org/S97548893',    'https://openalex.org/S7424199',
    'https://openalex.org/S138645024',   'https://openalex.org/S1016481467',  'https://openalex.org/S71057445',
    'https://openalex.org/S2736490681',  'https://openalex.org/S138274780',   'https://openalex.org/S165087003',
    'https://openalex.org/S51211322',    'https://openalex.org/S146592948',   'https://openalex.org/S42468459',
    'https://openalex.org/S30699777',    'https://openalex.org/S24510439',    'https://openalex.org/S157001289',
    'https://openalex.org/S200414547',   'https://openalex.org/S175526339',   'https://openalex.org/S127589907',
    'https://openalex.org/S68754933',    'https://openalex.org/S202681640',   'https://openalex.org/S4210220442',
    'https://openalex.org/S4210180838',  'https://openalex.org/S4210215428',  'https://openalex.org/S35917800',
    'https://openalex.org/S98137347',    'https://openalex.org/S87933477',    'https://openalex.org/S187348256',
    'https://openalex.org/S61446109',    'https://openalex.org/S80967739',    'https://openalex.org/S118082279',
    'https://openalex.org/S94746221',    'https://openalex.org/S188481820',   'https://openalex.org/S187261251',
    'https://openalex.org/S96897161',    'https://openalex.org/S39973823',    'https://openalex.org/S115783906',
    'https://openalex.org/S70424273',    'https://openalex.org/S61715262',    'https://openalex.org/S2764570952',
    'https://openalex.org/S2737998943',  'https://openalex.org/S46039370',    'https://openalex.org/S35062419',
    'https://openalex.org/S2737545834',  'https://openalex.org/S98232940',    'https://openalex.org/S4210173062',
    'https://openalex.org/S4306534686',  'https://openalex.org/S4210194517',  'https://openalex.org/S4306501641',
    'https://openalex.org/S106822843',   'https://openalex.org/S165709033',   'https://openalex.org/S25370267',
    'https://openalex.org/S107737141',   'https://openalex.org/S40252047',    'https://openalex.org/S149872823',
    'https://openalex.org/S93630570',    'https://openalex.org/S38024979',    'https://openalex.org/S10082577',
    'https://openalex.org/S1131227',     'https://openalex.org/S179906392',   'https://openalex.org/S122290859',
    'https://openalex.org/S7345157',     'https://openalex.org/S149422333',   'https://openalex.org/S2898181366',
    'https://openalex.org/S8194976',     'https://openalex.org/S147910473',   'https://openalex.org/S31690342',
    'https://openalex.org/S15174096',    'https://openalex.org/S84764624',    'https://openalex.org/S2764471594',
    'https://openalex.org/S18869182',    'https://openalex.org/S2764961298',  'https://openalex.org/S4306513461',
    'https://openalex.org/S88020226',    'https://openalex.org/S166870025',   'https://openalex.org/S169586356',
    'https://openalex.org/S48690275',    'https://openalex.org/S129839026',   'https://openalex.org/S138679565',
    'https://openalex.org/S31064911',    'https://openalex.org/S120433428',   'https://openalex.org/S92608454',
    'https://openalex.org/S4210202845',  'https://openalex.org/S120387555',   'https://openalex.org/S46746144',
    'https://openalex.org/S119437531',   'https://openalex.org/S37813251',    'https://openalex.org/S107749620',
    'https://openalex.org/S183135890',   'https://openalex.org/S182336501',   'https://openalex.org/S143413576',
    'https://openalex.org/S27614628',    'https://openalex.org/S4210222380',  'https://openalex.org/S2764878774',
    'https://openalex.org/S2764769492',  'https://openalex.org/S4210200929',  'https://openalex.org/S2898150413',
    'https://openalex.org/S4210219630',  'https://openalex.org/S96544531',    'https://openalex.org/S2737558550',
    'https://openalex.org/S116184629',   'https://openalex.org/S63571384',    'https://openalex.org/S161743634',
    'https://openalex.org/S185874209',   'https://openalex.org/S154084123',   'https://openalex.org/S153485742',
    'https://openalex.org/S90670110',    'https://openalex.org/S25376279',    'https://openalex.org/S72915307',
    'https://openalex.org/S169433491',   'https://openalex.org/S158706580',   'https://openalex.org/S195167216',
    'https://openalex.org/S188819509',   'https://openalex.org/S2765021630',  'https://openalex.org/S8759915',
    'https://openalex.org/S4460519',     'https://openalex.org/S2765058493',  'https://openalex.org/S27614628',
    'https://openalex.org/S181171746',   'https://openalex.org/S4210202650',  'https://openalex.org/S4210186262',
    'https://openalex.org/S2738526030',  'https://openalex.org/S4306528578',  'https://openalex.org/S150314616',
    'https://openalex.org/S192650101',   'https://openalex.org/S27273401',    'https://openalex.org/S942037226',
    'https://openalex.org/S40137911',    'https://openalex.org/S28036099',    'https://openalex.org/S96563387',
    'https://openalex.org/S2764791026',  'https://openalex.org/S151119180',   'https://openalex.org/S733369',
    'https://openalex.org/S105066276',   'https://openalex.org/S17986141',    'https://openalex.org/S124362806',
    'https://openalex.org/S33125897',    'https://openalex.org/S104805611',   'https://openalex.org/S175590358',
    'https://openalex.org/S4210206464',  'https://openalex.org/S132495599',   'https://openalex.org/S135713760',
    'https://openalex.org/S83616544',    'https://openalex.org/S4210196933',  'https://openalex.org/S4306520822',
    'https://openalex.org/S4210240912',  'https://openalex.org/S23791984',    'https://openalex.org/S4306520823',
    'https://openalex.org/S8802318',     'https://openalex.org/S104917558',   'https://openalex.org/S87560609',
    'https://openalex.org/S13144211',    'https://openalex.org/S91660768',    'https://openalex.org/S64418186',
    'https://openalex.org/S9435936',     'https://openalex.org/S141988568',   'https://openalex.org/S88198767',
    'https://openalex.org/S207420867',   'https://openalex.org/S193412822',   'https://openalex.org/S176081437',
    'https://openalex.org/S179213746',   'https://openalex.org/S151944519',   'https://openalex.org/S43621796',
    'https://openalex.org/S80823180',    'https://openalex.org/S57027881',    'https://openalex.org/S36718530',
    'https://openalex.org/S2765035532',  'https://openalex.org/S146242404',   'https://openalex.org/S200275961',
    'https://openalex.org/S102275153',   'https://openalex.org/S158932249',   'https://openalex.org/S4210184327',
    'https://openalex.org/S4306520556',  'https://openalex.org/S102499938',   'https://openalex.org/S79054089',
    'https://openalex.org/S137832324',   'https://openalex.org/S146344',      'https://openalex.org/S191319304',
    'https://openalex.org/S89389284',    'https://openalex.org/S171183513',   'https://openalex.org/S187626162',
    'https://openalex.org/S66201313',    'https://openalex.org/S11392764',    'https://openalex.org/S12175909',
    'https://openalex.org/S17740374',    'https://openalex.org/S184239247',   'https://openalex.org/S199605113',
    'https://openalex.org/S90149737',    'https://openalex.org/S51001188',    'https://openalex.org/S2530642067',
    'https://openalex.org/S4210177192',  'https://openalex.org/S55323383',    'https://openalex.org/S4210230240',
    'https://openalex.org/S2764362400',  'https://openalex.org/S2496055428',  'https://openalex.org/S4210191868',
    'https://openalex.org/S9936406',     'https://openalex.org/S4210215240',  'https://openalex.org/S15470582',
    'https://openalex.org/S13389975',    'https://openalex.org/S50366009',    'https://openalex.org/S4210168021',
    'https://openalex.org/S28278612',    'https://openalex.org/S130455057',   'https://openalex.org/S199447588',
    'https://openalex.org/S168863142',   'https://openalex.org/S135297974',   'https://openalex.org/S71968408',
    'https://openalex.org/S147692640',   'https://openalex.org/S94236332',    'https://openalex.org/S11810700',
    'https://openalex.org/S100176667',   'https://openalex.org/S27211427',    'https://openalex.org/S106098198',
    'https://openalex.org/S51698217',    'https://openalex.org/S2764742558',  'https://openalex.org/S2765018649',
    'https://openalex.org/S87168931',    'https://openalex.org/S2764571748',  'https://openalex.org/S2739293849',
    'https://openalex.org/S197465442',   'https://openalex.org/S2764845239',  'https://openalex.org/S52417371',
    'https://openalex.org/S86033158',    'https://openalex.org/S9731383',     'https://openalex.org/S64744539',
    'https://openalex.org/S141724154',   'https://openalex.org/S62009534',    'https://openalex.org/S22283869',
    'https://openalex.org/S190942573',   'https://openalex.org/S4210208415',  'https://openalex.org/S4210188777',
    'https://openalex.org/S55099511',    'https://openalex.org/S4210183760',  'https://openalex.org/S4210193178',
    'https://openalex.org/S2735686177']





# [proc_journal_dispatch(j, "always") for j in l_journals_to_dl[0:2]]
# [proc_journal_dispatch(j, "never") for j in l_journals_to_dl]


