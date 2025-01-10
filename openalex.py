import csv
import glob
import gzip
import json
import os
import time
import pdb
import gc
import re
import sys
import sqlite3

import numpy as np
import pandas as pd
import pyalex
from pyalex import Works, Authors, Sources, Institutions, Topics, Concepts, Publishers, Funders, config, invert_abstract

from flatten_openalex_jsonl import flatten_works, flatten_sources, init_dict_writer

from litanai import gd_bibtex, litanai, qry_oai_assess, gd_reltexts


import clickhouse_connect
import pickle
import subprocess





from globs import DIR_CSV, DIR_JOURNAL_GZIP, FILE_CAREER_PAPERS, PROJ_DIR, DBNAME
# from globs import *
from jutils import *


config.email
config.email = "johannes.ae@hotmail.de"
config.max_retries = 0
config.retry_backoff_factor = 0.1
config.retry_http_codes = [429, 500, 503]



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
        
        ingest_dispatcher(l_longworks, l_entities_to_ingest, switch_ingest, b_data_fresh, flatten_works)

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

    global DBNAME
    
    cmd = f"""clickhouse-client -d {DBNAME} --query "INSERT INTO {entity} FROM INFILE '{DIR_CSV}{entity}.csv.gz' COMPRESSION 'gzip' FORMAT CSV\""""

    return(cmd)



def pickle_entity (l_entities, entity_id, DIR_ENTITY_PICKLES):

    print(f"pickling {len(l_entities)} entities")

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


def ingest_dispatcher(l_entities, l_entities_to_ingest, switch_ingest, b_data_fresh, func_flatten):
    """
    flatten and ingest depending on switches

    Parameters:
        l_entities: list of entities to be ingested
        l_entities_to_ingest: list of entities to be ingested
        switch_ingest: whether to ingest or not
        b_data_fresh: whether the data is fresh
        func_flatten: function to flatten the entities
    """

    if (switch_ingest == "only_fresh" and b_data_fresh == True) or (switch_ingest == 'always'):
        print("flattening papers to csv")
        # flatten_works(l_entities)
        func_flatten(l_entities)
    
        print("ingesting works")
        ingest_csv(DIR_CSV, l_entities_to_ingest)

    # if switch_ingest == "only_fresh" and b_data_fresh == False:
    else:
        print("skip flattening and ingesting")
        # ingest_csv(DIR_CSV, l_entities_to_ingest)

    # if switch_ingest == "always":
        
    #     print("flattening papers to csv")
    #     flatten_works(l_entities)
    #     print("ingesting works")
    #     ingest_csv(DIR_CSV, l_entities_to_ingest)

    # if switch_ingest == 'never':
    #     print("skip flattening and ingesting")



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
    ingest_dispatcher(l_papers, l_entities_to_ingest, switch_ingest, b_data_fresh, flatten_works)
    
        

def proc_journal_info (id_concept, switch_ingest) :

    # breakpoint() 

    id_concept_short = id_concept.replace('https://openalex.org/', '')
    print(f"id_journal_short: {id_concept_short}")

    # convert_dld_file(id_concept_short)
    

    # FIXME: proper paths
    if id_concept_short + ".json.gz" not in os.listdir(DIR_JOURNAL_GZIP):
        print("downloading journal info")
        l_journals = gl_journal_info(id_concept)
        pickle_entity(l_journals, id_concept_short, DIR_JOURNAL_GZIP)
        b_data_fresh = True
    else:
        if switch_ingest == "always":
            l_journals = pickle_load_entity(id_concept_short, DIR_JOURNAL_GZIP)
        else:
            l_journals = []
        print(f"retrieved {len(l_journals)} from file")
        b_data_fresh = False

    # print("flattening journal info to csv")
    # flatten_sources(l_journals)

    # j = l_journals[18] # BJS
    # topics = j['topics']

    # [print(j['display_name']) for j in l_journals[0:40]]
        
    # print("ingesting journals")
    l_entities_journals = ["sources", "sources_counts_by_year", 'sources_ids', 'sources_topics']
    # ingest_csv(DIR_CSV, l_entities_journals)

    ingest_dispatcher(l_journals, l_entities_journals, switch_ingest, b_data_fresh, flatten_sources)

        

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




def ingest_new_journals ():
    # breakpoint()

    ibis.options.interactive = True

    ## get the journals that are already downloaded
    l_journals_dld_prep1 = os.listdir(DIR_JOURNAL_GZIP)
    l_journals_dld_prep2 = [re.sub(r'\.json\.gz', '', i) for i in l_journals_dld_prep1]
    l_journals_dld_prep3 = [i for i in l_journals_dld_prep2 if i[0].lower() == 's']

    l_journals_dld = set([s.split('_', 1)[0] for s in l_journals_dld_prep3])

    
    ## get the journals that are already ingested
    conch = ibis.connect('clickhouse://localhost/litanai')
    tw = conch.table("works")
    l_journals_ingstd_prep1 = tw.select(_.source_id).distinct().to_pandas()['source_id'].to_list()

    l_journals_ingstd = set(lmap(lambda x:re.sub(r'https://openalex.org/', '', x), l_journals_ingstd_prep1))

    ## get the journals that are downloaded and not ingested
    l_journals_to_ingest = l_journals_dld - l_journals_ingstd
    print(len(l_journals_to_ingest))

    # return(l_journals_to_ingest)
    # xx = list(l_journals_to_ingest)

    # ingest them
    lmap(lambda x:proc_journal_dispatch(x, "always"), l_journals_to_ingest)


def get_sim_journals (min_topic_cnt_ttl = 100, min_journal_topic_cnt = 25, min_topics_met = 5) :

    "starts from my most used journals, then gets their topics, then gets other journals which also mention them"

    # min_topic_cnt_ttl: how many times a topic has to be mentioned in a journal to make it substantial
    # min_journal_topic_cnt: how many times a new journal has to have a topic to be included
    # min_topics_met: only include journals which mention at least that many of my topics
    conch = ibis.connect('clickhouse://localhost/litanai')

    # breakpoint()
    tmyj = conch.table('bib_myj') # get my refs
    tst = conch.table('sources_topics') # get source-topic links
    tsrc = conch.table('sources') # get sources
    tw = conch.table('works')  # get works
    txj = tw.group_by('source_id').aggregate(_.source_id.count()) # get existing journals

    # get the main topics of my most-used journals
    t_tpcs = (tst.inner_join(tmyj, tst.source_id == tmyj.id)
          .group_by(_.topic_id)
          .aggregate(topic_count_sum = _.topic_count.sum(), topic_prop_mean = _.topic_prop.mean())
          .filter(_.topic_count_sum > min_topic_cnt_ttl))

    # get the journals that also have the topics that I use 
    d_journals = (
        t_tpcs.inner_join(tst, t_tpcs.topic_id == tst.topic_id)
        .filter(_.topic_count > min_journal_topic_cnt) # only use journals that use them substantially
        .group_by(_.source_id) # aggregate to journal
        .aggregate(topic_cnt_sum_jrnl = _.topic_count.sum(), n_topics_met = _.topic_id.count())
        .filter(_.n_topics_met > min_topics_met) # only 
        .anti_join(txj, _.source_id == txj.source_id) # yeet my journals
        # join with sources to get info
        .join(tsrc.select('id', 'display_name', 'works_count', 'cited_by_count'), _.source_id == tsrc.id)
        .mutate(cites_per_work = _.cited_by_count/_.works_count))

    return(d_journals)


def proc_sources_h_index (vlu_start, vlu_end, switch_ingest):
    'downloads section of source info based on h-index range'
    
    # breakpoint()

    id_short = f"info_sources_{vlu_start}_{vlu_end}"
    print(id_short)
    if id_short + ".json.gz" not in os.listdir(DIR_JOURNAL_GZIP):

        qry = (Sources().filter(summary_stats = {'h_index' : f">{vlu_start - 1}"})) # need to substract 1 since geq doesn't exist

        # if not final query: add upper boundary
        if pd.notna(vlu_end):
            qry = (qry.filter(summary_stats = {'h_index' : f"<{vlu_end}"}))

        nbr_sources = qry.count()
        print(nbr_sources)

        pager = qry.paginate(per_page = 200, n_max = None)

        l_sources = dl_pages(pager, nbr_sources)
        pickle_entity(l_sources, id_short, DIR_JOURNAL_GZIP)
        b_data_fresh = True

    else:
        if switch_ingest == "always":
            l_sources = pickle_load_entity(id_short, DIR_JOURNAL_GZIP)
        else:
            l_sources = []
        print(f"retrieved {len(l_sources)} from file")
        b_data_fresh = False


    l_entities_journals = ["sources", "sources_counts_by_year", 'sources_ids', 'sources_topics']
    ingest_dispatcher(l_sources, l_entities_journals, switch_ingest, b_data_fresh, flatten_sources)


def dl_all_the_sources() :

    conch = ibis.connect('clickhouse://localhost/litanai')
    tsrc = conch.table("sources")

    qntls = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    qntl_vlus = tsrc.aggregate(qntl_border = tsrc.h_index.quantile(qntls)).execute()
    l_qntl_vlus = [round(i) for i in qntl_vlus['qntl_border'].to_list()[0]]

    d_qntls = pd.DataFrame({'qntl' :qntls, "vlu":l_qntl_vlus})
    d_qntls['vlu_lag'] = d_qntls['vlu'].shift(-1)
    print(d_qntls)

    l_srccfgs = d_qntls.apply(lambda r :{'vlu_start': r['vlu'], 'vlu_end' : r['vlu_lag']}, axis = 1).tolist()
    l_srccfgs.reverse()

    l_srccfgs = [{'vlu_start' : 1, 'vlu_end' : 2}, {'vlu_start' : 0, 'vlu_end' : 1}]

    [proc_sources_h_index(c['vlu_start'], c['vlu_end'], 'only_fresh') for c in l_srccfgs]





def gd_journals(l_works) :
    "get journals from a list of works"

    print(len(l_works))
    l_works_split = split_list(l_works, 25)

    nbr = 0
    l_sources = []
    llw = []

    for l in l_works_split:
        print(nbr)
        llw.append(Works()[l])
        nbr += 1

    lw = flatten_list(llw)

    ls = []
    for w in lw:
        if prim_loc := w.get('primary_location'):
            if src := prim_loc.get('source'):
                if dispname := src.get('display_name'):
                    ls.append(src)

    ls_ids = list(set([s['id'] for s in ls]))

    return(ls_ids)


def gl_new_journals_from_bib():
    """
    Gets journals which have articles in the references databases and haven't been downloaded yet
    Returns:
        list: list of journal ids
    """

    dt_bibtex = gd_bibtex()

    tlit = move_tbl_to_ch(dt_bibtex, 'temp_bibtex', conch)
    tlit.filter(_.doi.notnull()).select('key', 'doi')

    tlit_doi = (tlit.filter(_.doi.notnull())
                .select(_.key,
                        doi = ibis.ifelse(_.doi.ilike('https://doi.org/%'), _.doi,
                                          'https://doi.org/' + _.doi)))

    # check that this looks good
    # (tlit_doi.mutate(doi_start = _.doi.substr(0, 16))
    #  .group_by(_.doi_start)
    #  .aggregate(cnt = _.doi.count()))

    tlit_fltrd = tlit_doi.filter(_.doi.notin(tw.doi))

    l_doi = tlit_fltrd.execute()['doi'].to_list()

    # get works
    # lw = lmap(lambda x: try: Works()[x] except: [], l_doi)

    lw = []
    for d in l_doi:
        try:
            lw.append(Works()[d])
        except:
            pass
    
    # get sources
    l_src = list(set(gd_journals(list(set(lmap(lambda x: x['id'], lw))))))

    # get existing journals
    t_xj = tw.filter(tw.source_id.isin(l_src)).group_by('source_id').aggregate(nbr = _.source_id.count())
    l_xj = t_xj.execute()['source_id'].to_list()

    l_journals_to_dl = list(set(l_src) - set(l_xj))

    return(l_journals_to_dl)



        
        
# * query database

# ** get works based on search strings

# search_strings_career = ['%career%', '%life-course%', '%lifecourse%']
# search_strings_subject = ['%artist%', '%musician%', '%poet%', '%painter%']
# search_strings_outcome = ['%recogni%', '%reputation%', '%consecrat%', '%canoniz%']


# qry = (tw.filter(tw.abstract_text.ilike(search_strings_career),
#                  tw.abstract_text.ilike(search_strings_subject),
#                  tw.abstract_text.ilike(search_strings_outcome))
#        .select(_.cited_by_count, _.display_name, _.publication_year, key = _.id, text = _.abstract_text))
#        # .filter(_.cited_by_count > 5))

# raise RuntimeError("it's time to stop.")

# ** get works related (according to OA) to relevant works 

# tcree = gt_cree()
# twrw = conch.table("works_related_works")


# # get unique related ones
# qry_relw = (twrw.filter(twrw.work_id.isin(tcree.work_id))
#  .select(_.related_work_id).distinct())
 
# qry_relw.count()

# qry = (tw.filter(tw.id.isin(qry_relw.related_work_id)) #  get related that exists
#        .select(_.cited_by_count, _.display_name, _.publication_year, key = _.id, text = _.abstract_text))

# ** get works about exhibition, career, artist

qry = (tw.filter(tw.abstract_text.ilike(['%exhibition%', '%museum%', '%display%']),
                 tw.abstract_text.ilike(['%career%', '%auction%']),
                 tw.abstract_text.ilike('%artist%'))
       # .count())
       .select(_.cited_by_count, _.display_name, _.publication_year, key = _.id, text = _.abstract_text))


prompt_oai = """
you will read a short text, which generally is the abstract of a scientific article, but it might also be the first few paragraphs. you have to evaluate whether the text is about artist careers, in particular whether it deals with factors leading to successful careers, which might be phrased as success, recognition, reputation, consecration, or canonization. you have to assess a number of things:
- relevance, i.e. whether the text is about artist careers. this should be a decimal number ranging from 0 to 1 based on how relevant the text is for artistic careers
- discipline, which should be your judgement of what discipline the article is from (e.g. sociology, art history, economics)
- time period: which time period the artists are from (if the text is not about artists, return NA)
- methodology: whether the text is quantitative or qualitative (return NA if not applicable)
return these values as a json-dictionary with the keys 'relevance', 'discipline', 'time_period', and 'methodology'

the text follows below: 
"""

# dt_reltexts = gd_reltexts(str(ibis.to_sql(qry)))
# dx = litanai(dt_reltexts, prompt_oai, qry_oai_assess, "cree_find", False)


# * download all journal info
# proc_sources_h_index(380, 390, "never")


# dl_all_the_sources()

# * download new journals
# qry = get_sim_journals(min_topic_cnt_ttl = 100, min_journal_topic_cnt = 20, min_topics_met = 6)
# # xx.filter(_.works_count < 19000).aggregate(_.works_count.sum())

# l_journals = qry.select('source_id').execute()['source_id'].to_list()
# print(len(l_journals))
# [proc_journal_dispatch(j, "never") for j in l_journals]

# l_journals = gl_new_journals_from_bib()
# [proc_journal_dispatch(j, "only_fresh") for j in l_journals]



# * ingest new journals

# ingest_new_journals()


# * download single works

# l_works = ['https://openalex.org/W618036604', 'https://openalex.org/W4240439380']

# l_works = Works()[l_works]

# flatten_works(l_works)
# ingest_dispatcher(l_works, ['works', 'works_related_works', 'works_referenced_works'], 'always', True, flatten_works)
