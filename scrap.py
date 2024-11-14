# * initial query testing

# with open(DIR_PROJ + "fasche_openparse.txt", "w") as file:
#     file.write("\n".join(l_nodes_text))



# display doesn't work in emacs
# pdf = openparse.Pdf(test_doc)

# pdf.display_with_bboxes(
#     parsed_basic_doc.nodes,
# )


with open(DIR_PROJ + "fasche_mupdf.txt", "w") as file:
    # Write text to the file
    file.write(doc_txt)


# * datatable testing
# from math import sqrt

# import datatable as dt
# from datatable import f, min, max

# DIR_PROJ = "/home/johannes/Dropbox/proj/litanai/"


dt.Frame(dtx[['key', 'len']])

dt.Frame(key = ["a", 'b', 'c\nkk'], b = [1,2,3])

dt.Frame(list(dtx[['key', 'len']].head()))

dt2 = dt.Frame(key = list(dtx['key']), len = list(dtx['len']),
               n_occur = list(dtx['n_occur']), text = list(dtx['text']))

def tester (a):
    if True:
        return(a+2)

def tester2 (b): return(sqrt(b))

dt2[list(range(0,6)),{'sv':f.n_occur/2, 'svj':f.n_occur**0.5,
                      'lll':tester(f.n_occur), 'n_occur':f.n_occur}]
                      # 'ooo':tester2(f.n_occur)}]


# * polars testing
pd = pl.DataFrame(dtx)
                      
with pl.Config(tbl_formatting=""):
    print(pd)



dtx[dtx['len'] < 100000]

# * bibtex clickhouse test, but getting mauled by schema requirements

df_bib = gd_bibtex()
df_bib.columns


cmd_create_tbl = """create table lit (
`key` String,
`author` String,
`title` String,
`year` UInt32,
doi String
)
ENGINE = MergeTree()
ORDER BY key
"""

client.command(cmd_create_tbl)

client.insert_df("lit", df_bib)

# * openalex API tests

Works().count()

l_works = Works().filter(publication_year = 2020).count()

Source

w = Works()["W3128349626"]

w_komarova = Works()["W2900887489"]
w_komarova.keys()

# get journal
w_komarova['primary_location']['source']['id']


# get all the poetics
l_poetics = Works().filter(primary_location={"source": {"id" :'https://openalex.org/S98355519'}}).get()


l_poetics_pager = Works().filter(primary_location={"source": {"id" :'https://openalex.org/S98355519'}}).paginate(per_page=200)

l_w_poetics = []

for page in l_poetics_pager:
    print(len(page))
    l_w_poetics.append(page)



l_w_poetics_flat = [x for xs in l_w_poetics.copy() for x in xs]
len(l_w_poetics_flat)


l_papers_poetics = gl_journal_papers('https://openalex.org/S98355519')
pickle_journal(l_papers_poetics, "s98355519", DIR_JOURNAL_PICKLES)
flatten_works(l_papers_poetics)

l_papers_asr = gl_journal_papers('https://openalex.org/s157620343')

pickle_journal(l_papers_asr, "s157620343", DIR_JOURNAL_PICKLES)
l_papers_asr_unpickled = pickle_load_journal('s157620343', DIR_JOURNAL_PICKLES)

flatten_works(l_papers_asr)

ingest_csv(DIR_CSV)

nbr_papers = Works().filter(primary_location= {"source": {"id" :'https://openalex.org/s157620343'}}).count()


# * figuring out sqlalchemy

t_works = metadata.tables['works']

t_works = Table('works', metadata, autoload_with = engine)

query = select(t_works.c.id, t_works.c.cited_by_count).where(t_works.c.publication_year > 2015)
res = session.execute(query)

rows = res.fetchall()
len(rows)
rows[0:5]


pd.DataFrame(rows)
pd.read_sql(query, engine)


# works_present = ch_client.query_df("select distinct work_id from works_related_works")
# works_related = ch_client.query_df("select distinct related_work_id from works_related_works")

# setdiff = set(works_related['related_work_id']) - set(works_present['work_id'])
# len(setdiff)


# def get_journals ():


# * downloading individual journals
[proc_journal(j['id']) for j in l_journals_to_dl]

proc_journal('https://openalex.org/S4306463855')
proc_journal('https://openalex.org/S31225034')


    
proc_journal('https://openalex.org/s157620343')

l_journals = ['https://openalex.org/s98355519', 'https://openalex.org/s157620343']  # poetics, ASR

[proc_journal(journal) for journal in l_journals]





Sources().count()

asr = Sources()["S157620343"]


# * api testing


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


# https://docs.google.com/spreadsheets/d/1LBFHjPt4rj_9r0t0TTAlT68NwOtNH8Z21lBMsJDMoZg/edit?gid=575855905#gid=575855905

# sociology: 50k
Sources().filter(concept = {'id' : 'https://openalex.org/C144024400'}, type = "journal").count()

# social science: 5.7k huh
Sources().filter(concept = {'id' : 'https://openalex.org/C36289849'}, type = "journal").count()

# polsci
Sources().filter(concept = {'id' : 'https://openalex.org/c17744445'}, type = "journal").count()

# nonprofit
https://openalex.org/c2993714901

# 
https://openalex.org/c2986758647

l_concepts_short = ["c2986758647", "c2993714901", "C144024400", "C36289849", "c17744445"]
l_concepts = ['https://openalex.org/' + i for i in l_concepts_short]

for c in l_concepts:
    dname = Concepts()[c].get('display_name')
    journal_cnt = Sources().filter(concept = {'id' : c}, type = "journal").count()
    print(f"{dname}:{journal_cnt}")


# art history
Sources().filter(concept = {'id' : 'https://openalex.org/c52119013'}, type = "journal").count()


l_journals = gl_journal_info('https://openalex.org/C48798503')
flatten_sources(l_journals)
l_entities_journals = ["sources", "sources_counts_by_year", 'sources_ids']
ingest_csv(DIR_CSV, l_entities_journals)

# len(l_journals)

# * get my journals

dt_bibtex = gd_bibtex()

dt_bibtex_res = (dt_bibtex[dt_bibtex['journal'].notna()]
 .groupby('journal').size().reset_index(name='count')
 .sort_values(by='count', ascending = False)
                 .query('count > 2'))

# search journal_id for each journal in dt_bibtex, and put journal_id in new column journal_id
# return missing value if journal not found

def search_source_from_name (name):
    l_res = Sources().search(name).get()
    if len(l_res) > 0:
        return l_res[0]['id']
    else:
        return None


dt_bibtex_res['journal_id'] = (dt_bibtex_res['journal'].apply(lambda x: search_source_from_name(x)))


l_res = Sources().search("European Journal of Cultural Studies").get()

t_works = Table('works', metadata, autoload_with = engine)

qry = (select(t_works.c.source_id, func.count(t_works.c.source_id).label('cnt')
        ).group_by(t_works.c.source_id))

dt_works_db = pd.read_sql(qry, engine)

dt_works_db.to_string()


print(dt_works_db)

# remove all sources from dt_bibtex that are already in dt_works_db

# also filter out all entries where journal_id is None
dt_bibtex_res2 = (dt_bibtex_res[~dt_bibtex_res['journal_id'].isin(dt_works_db['source_id'])]
                  .query('journal_id.notna()'))

# print all the journal_ids from dt_bibtex_res2


pd.set_option('display.max_rows', None)  # Display all rows
dt_bibtex_res2['journal_id']
pd.reset_option('display.max_rows')




# * see which journals are in the database

t_sources = Table('sources', metadata, autoload_with = engine)

# merge t_works with t_sources on source_id to get names of unique entries
# also get number of entries for each source

# first get counts of all sources, then merge with sources table to get names
qry1 = (select(t_works.c.source_id, func.count(t_works.c.source_id).label('cnt'))
       .group_by(t_works.c.source_id)).subquery()

qry2 = (select (qry1.c.source_id, t_sources.c.display_name, qry1.c.cnt)
        .join(t_sources, qry1.c.source_id == t_sources.c.id).distinct()).subquery()
.join(t_sources, t_works.c.source_id == t_sources.c.id))

# see which entries are in qry1 but not in qry2
qry3 = select(qry1.c.source_id).except_(select(qry2.c.source_id)).subquery()

pd.read_sql(select(qry1), engine)
pd.read_sql(select(qry2), engine)
pd.read_sql(select(qry3), engine)


Works().filter(primary_location= {"source": {"id" :"https://openalex.org/S4210172589"}}).count()

Sources().["https://openalex.org/S4210172589"].




# * testing saving as json rather than csv, but that's even bigger


journal_year_id = "S125754415_2024"
l_longworks = pickle_load_entity(journal_year_id, DIR_JOURNAL_PICKLES)

DIR_ENTITIES_JSON = "/run/media/johannes/data/litanai/entities_json"

pickle_entity(l_longworks, journal_year_id, DIR_ENTITIES_JSON)

with open(os.path.join(DIR_ENTITIES_JSON, journal_year_id) + ".json", 'w') as fx:
    json.dump(l_longworks, fx)

with gzip.open(os.path.join(DIR_ENTITIES_JSON, journal_year_id) + ".json.gz", 'wt') as fx:
    json.dump(l_longworks, fx)


with gzip.open(os.path.join(DIR_ENTITIES_JSON, journal_year_id) + ".json.gz", 'rt') as fx:
    l_longworks_fgz = json.load(fx)

    

xx = l_longworks_fgz[300]
xx.get('id')


import brotli

t1 = time.time()
compressed_data = brotli.compress(json.dumps(l_longworks).encode('utf-8'))
t2 = time.time()

t2-t1

with open(os.path.join(DIR_ENTITIES_JSON, journal_year_id) + "_brotli.json.br", 'wb') as f:
    f.write(compressed_data)


import json
import zstandard as zstd

# Example dictionary


# Serialize and compress with Zstandard
cctx = zstd.ZstdCompressor()

t1 = time.time()
compressed_data = cctx.compress(json.dumps(l_longworks).encode('utf-8'))
t2 = time.time()
t2-t1

# Save to a file
with open(os.path.join(DIR_ENTITIES_JSON, journal_year_id) + "_zstd.json.gz", 'wb') as f:
    f.write(compressed_data)



import lzma

# Example dictionary


# Serialize and compress with LZMA
t1 = time.time()
compressed_data = lzma.compress(json.dumps(l_longworks).encode('utf-8'))
t2 = time.time()

t2-t1

# Save to a file
with open(os.path.join(DIR_ENTITIES_JSON, journal_year_id) + "_lzma.json.gz", 'wb') as f:
    f.write(compressed_data)



# * search

l_search = Works().search("The Blackwell Companion to Organization").get()

l_search = Works().search("Interorganizational Cognition and Interpretation").get()
len(l_search)
[print(i['display_name']) for i in l_search]
l_search[0]['id']

# * query testing for works

DIR_RES = "/run/media/johannes/data/litanai/results/"

# get all works related to artist careers

t_works = Table('works', metadata, autoload_with = engine)
t_sources = Table('sources', metadata, autoload_with = engine)

search_strings_career = ['career', 'life-course', 'lifecourse']
search_strings_subject = ['artist', 'musician', 'poet', 'painter']
search_strings_outcome = ['recogni', 'reputation', 'consecrat', 'canoniz']


career_conditions = [t_works.c.abstract_text.like(f'%{s}%') for s in search_strings_career]
subject_conditions = [t_works.c.abstract_text.like(f'%{s}%') for s in search_strings_subject]
outcome_conditions= [t_works.c.abstract_text.like(f'%{s}%') for s in search_strings_outcome]

col_lens = [((func.length(t_works.c.abstract_text) - func.length(func.replace(t_works.c.abstract_text, s, '')))
                / cast(len(s), Numeric(10,4))).label(f"cnt_{s}")
            for s in search_strings_subject + search_strings_outcome + search_strings_career]


qry = select(t_works.c.id, t_works.c.display_name, t_works.c.source_id, t_works.c.abstract_text, *col_lens).where(
    and_(
        func.length(t_works.c.abstract_text) > 10,
        or_(*career_conditions),
        or_(*subject_conditions),
        or_(*outcome_conditions))).subquery()

pd.read_sql(select(func.count(qry.c.id)), engine)

dt_careers = pd.read_sql(select(qry), engine)


pd.read_sql(select(qry).limit(100), engine).to_csv(DIR_RES + "artist_careers.csv")

# FIXME 1: yeet duplicate journals
# FIXME 2: make sure all works have source with 
qry_jrnl = (select(qry.c.id, t_sources.c.display_name, t_sources.c.id.label('source_id'))
            .join(t_sources, qry.c.source_id == t_sources.c.id)).subquery()

# group by journal
qry_jrnl_cnt = (select(
    qry_jrnl.c.display_name,
    qry_jrnl.c.source_id,
    func.count(qry_jrnl.c.source_id))
                .group_by(qry_jrnl.c.source_id)
                .order_by(desc(func.count(qry_jrnl.c.id))))

pd.read_sql(qry_jrnl_cnt, engine)

pd.read_sql(select(qry_jrnl), engine)
pd.read_sql(select(distinct(qry.c.id)), engine)
            



pd.read_sql(qry, engine)


    
qry = select(
    t_works.c.id,
    t_works.c.display_name,
    t_works.c.abstract_text,
    *[(func.length(t_works.c.abstract_text) - func.length(func.replace(t_works.c.abstract_text, s, ''))) / 
      func.length(s).cast(Float64).label(f"subject_count_{s}") 
      for s in search_strings_subject],
    *[(func.length(t_works.c.abstract_text) - func.length(func.replace(t_works.c.abstract_text, o, ''))) / 
      func.length(o).cast(Float64).label(f"outcome_count_{o}") 
      for o in search_strings_outcome]
).where(
    and_(
        or_(*subject_conditions),
        or_(*outcome_conditions)
    )
).subquery()

# * explore more works info

w = Works()["W3128349626"]
w = Works()['W2067091741']

l_search = Works().search("Interorganizational Cognition and Interpretation").get()
w = l_search[0]

w.keys()


nested_dict = {
    'key': {
        'keyb': [1, 2, 3],
        'keyc': {
            'keyd': 'value',
            'keye': [1.0, 2.0]
        }
    },
    'keyf': 42,
    'keyg': True
}

print_dict_structure(nested_dict)

print_dict_structure(w)

wt = w['topics']

flatten_works([wt])

wc = w['concepts']
            

# * get similar journals


dt_bibtex = gd_bibtex()

dt_bibtex_res = (dt_bibtex[dt_bibtex['journal'].notna()]
                 .groupby('journal').size().reset_index(name='count'))
 



# bibtex to write to clickhouse (toch)
dt_bibtex_res_toch = dt_bibtex_res[dt_bibtex_res['journal'].notna()]




# for some reson specifying this breaks ibis
# tbl = ibis.table(name = "temp_bib2", database = "litanai",
#                  schema = {'journal' : 'string', 'count' : 'int32'})
# database = 'litanai',
# engine = 'MergeTree', order_by = 'journal')



ch_client = clickhouse_connect.get_client(database = "litanai")

create_table_query = '''
CREATE TABLE temp_bibtex_res (
    journal String,
    count UInt32,
    journal_id String
) ENGINE = MergeTree() ORDER BY journal_id
'''

# Execute the create table query
ch_client.query(create_table_query)

ch_client.insert_df("temp_bibtex_res", dt_bibtex_res_toch)

t_bibtex = Table('temp_bibtex_res', metadata, autoload_with = engine)
t_sources = Table('sources', metadata, autoload_with = engine)
t_st = Table('sources_topics', metadata, autoload_with = engine)

# get my topics (mt)
qry_mt = (select(t_st.c.topic_id, t_st.c.source_id, t_st.c.topic_count, t_st.c.topic_prop)
          .join(t_bibtex, t_bibtex.c.journal_id == t_st.c.source_id)
          .where(t_st.c.topic_prop < 0.8)).subquery()

pd.read_sql(select(qry_mt), engine)
dtx = pd.read_sql(select(qry_mt), engine)
print(qry_mt.compile(compile_kwargs={"literal_binds": True}))


# get my topics, gropued (mtg)
qry_mtg = (select(qry_mt.c.topic_id, func.avg(qry_mt.c.topic_prop).label('prop_avg'))
                     .group_by(qry_mt.c.topic_id)
           .order_by(desc(func.sum(qry_mt.c.topic_prop)))).subquery()

# get all journals that mention my topics, and get the difference in topic proportion
qd = (select(t_st.c.topic_id, t_st.c.source_id, t_st.c.topic_prop, qry_mtg.c.prop_avg,
             func.abs(t_st.c.topic_prop - qry_mtg.c.prop_avg).label('prop_diff'))
      .where(t_st.c.topic_prop < 0.8)
      .join(qry_mtg, t_st.c.topic_id == qry_mtg.c.topic_id)).subquery()



# summarize journals, get top ones
qj = (select(qd.c.source_id, func.avg(qd.c.prop_diff).label('sum_diff'),
       func.count(qd.c.source_id).label('n_topics')).group_by(qd.c.source_id)
      .order_by(func.avg(qd.c.prop_diff))).subquery()

pd.read_sql(select(func.count(qj.c.source_id)), engine)

# get final query: yeet the journals that are already in the database
qf = select(qj).where(qj.c.source_id.notin_(select(distinct(t_works.c.source_id)))).subquery()

# get the final results with journal names
qfn = select(qf, t_sources.c.display_name, t_sources.c.works_count).join(t_sources, qf.c.source_id == t_sources.c.id).subquery()

pd.read_sql(select(func.count(qj.c.source_id)), engine)
pd.read_sql(select(func.count(qfn.c.source_id)), engine)
pd.read_sql(select(qf).where(qf.c.n_topics > 5).limit(100), engine)

pd.set_option('display.max_rows', None)  # Display all rows
pd.read_sql(select(qfn).where(qfn.c.n_topics > 5).limit(100), engine)
pd.reset_option('display.max_rows')


# oof these have like nothing to do with my topics -> rather get the rest of my journals


dt_bibtex_res2['journal_id']


sx = Sources()['S10441410']


t_works



# next: use the journal ids to get topics and then similar journals
# * use ngram distance 

dt_bibtex = gd_bibtex()

dt_bibtex_res = (dt_bibtex[dt_bibtex['journal'].notna()]
 .groupby('journal').size().reset_index(name='count')
 .sort_values(by='count', ascending = False))

dt_works_there = pd.read_sql(select(distinct(t_works.c.source_id), t_sources.c.display_name)
                             .group_by(t_works.c.source_id)
                             .join(t_sources, t_works.c.source_id == t_sources.c.id), engine)

                             



# * prql

ch_client.query("set dialect = 'clickhouse'")
pd.read_sql("select count(*) from works", engine)



ch_client = clickhouse_connect.get_client(database = "litanai")
ch_client.command("set dialect = 'prql'")

pd.read_sql("""
from sources_topics
select topic_id 
take 10""", engine)

pd.read_sql("""
from works
select display_name
take 10;""", engine)

ch_client.query_df("""
from sources_topics
take 10
""")

ch_client.query("""
from sources_topics
aggregate
{ct = count source_id}
""")


from clickhouse_driver import Client
client = Client(host = 'localhost', database = 'litanai')
client.execute("show tables")

client.execute("set dialect = 'prql'")

cmd = """from sources_topics
select topic_id
take 10"""

select display_name
take 10

from sources
select display_name
take 10

from sources
aggregate {
    ct = count sources_id}

select count(*)


client.execute(cmd)



cmd = """from sources_topics
aggregate {
ct = count source_id}"""

client.execute("from sources_topics select topic_id take 10")


ch_client.query("from sources")

# * ibis


import ibis
from ibis import desc, _
ibis.options.interactive = True
ibis.options.interactive = False


con = ibis.connect('clickhouse://localhost/litanai')
tsrc = con.table('sources')
tbib = con.table('temp_bibtex_res')
tst = con.table('sources_topics')

# (tsrc.filter(tsrc.display_name == 'American Sociological Review')
#  .select('id', 'display_name'))

qx = (tst.select('topic_id', 'source_id', 'topic_count', 'topic_prop')
        .inner_join(tbib, tst.source_id == tbib.journal_id)
        .filter(tst.topic_prop < 0.8)
        .order_by(tst.topic_count.desc()))

qy = (qryx.group_by('topic_id')
      .aggregate(prop_avg = qryx.topic_prop.mean())
      .order_by(_.prop_avg.desc()))

qz = (qy.inner_join(tst.filter(tst.topic_prop < 0.8), "topic_id")
      .mutate(prop_diff = tst.topic_prop - qy.prop_avg).
      select('topic_id', 'source_id', 'topic_prop', 'prop_avg', 'prop_diff'))


(qz.group_by('source_id')
 .aggregate(sum_diff = qz.prop_diff.mean(), n_topics = qz.source_id.count())
 .order_by(ibis.desc('sum_diff'))).count()

print(ibis.to_sql(qz))

# combine to one query with chaining -> nice

qry = (tst.select('topic_id', 'source_id', 'topic_count', 'topic_prop')
 .inner_join(tbib, _.source_id == tbib.journal_id)
 .filter(_.topic_prop < 0.8)
 .group_by('topic_id')
 .aggregate(prop_avg = _.topic_prop.mean())
 .inner_join(tst.filter(_.topic_prop < 0.8), "topic_id")
 .mutate(prop_diff = (_.topic_prop - _.prop_avg).abs())
 .group_by('source_id')
 .aggregate(sum_diff = _.prop_diff.mean(), n_topics = _.source_id.count())
 .order_by(ibis.asc('sum_diff'))
 .left_join(tsrc.select('id', 'display_name'), _.source_id == tsrc.id))



dtx2 = (tst.select('topic_id', 'source_id', 'topic_count', 'topic_prop')
 .inner_join(tbib, tst.source_id == tbib.journal_id)
 # .order_by(tst.topic_count.desc())
 .filter(tst.topic_prop < 0.8)).to_pandas()

print(ibis.to_sql(qry))




# ** match rest of my journals
dt_bibtex = gd_bibtex()

dt_bibtex_res = (dt_bibtex[dt_bibtex['journal'].notna()]
                 .groupby('journal').size().reset_index(name='count_occ'))


con.drop_table("bib2")
con.create_table(name = "bib2", schema = ibis.schema({'journal' : 'string', 'count_occ' : 'int'}))

con.insert('bib2', dt_bibtex_res)

tbib2 = con.table('bib2')


tbib2
tsrc.select('id', 'display_name')

@ibis.udf.scalar.builtin(name="jaro_winkler_similarity")
def jw_sim(a: str, b: str) -> float:
   ...

@ibis.udf.scalar.builtin(name="ngramDistanceCaseInsensitive")
def ngdci(a: str, b:str) -> float:
    ...



tcross = (tbib2.cross_join(tsrc.select('id', 'display_name'))
          .mutate(sim = ngdci(_.journal, _.display_name)))

tminsim = (tcross.group_by('journal')
           .aggregate(min_sim = _.sim.min()))

# data.table with journals

# huh ok can use duckdb for local tables
conduck = ibis.connect("duckdb://")

dtj = conduck.create_table('dtj',
    tcross.inner_join(
        tminsim,
        [tcross.journal == tminsim.journal, tcross.sim == tminsim.min_sim]).to_pandas(), overwrite = True)


# existing journals
tw = con.table('works')

dt_xj = conduck.create_table(
    'dt_xj',
    tw.group_by('source_id').aggregate(cnt = _.source_id.count()).to_pandas(),
    overwrite = True)

ds = (dtj.anti_join(dt_xj, dtj.id == dt_xj.source_id)
      .filter(_.min_sim < 0.1)
      .order_by(desc('count_occ'))
      .filter(_.journal != "BMJ")).execute()


# get list of journal ids
l_jids = ds['id'].to_list()

l_jids_oa = [l_jids[i:i + 25] for i in range(0, len(l_jids), 25)]

l_source_info = lmap(lambda x: Sources()[x], l_jids_oa)

l_source_info_flat = flatten_list(l_source_info)

d_s_more = pd.DataFrame(lmap(lambda x: {'source_id' : x['id'],
                             'works_count' : x['works_count'],
                             'display_name' :x['display_name']},
                  l_source_info_flat))

d_s_more['works_count'].sum()

len(set(d_s_more['source_id']) - set(dt_xj.to_pandas()['source_id']))
len(set(d_s_more['source_id']))

d_s_more.iloc[285]
split_list(list(d_s_more['source_id']),3)




# TODO: download all these, should take ~1.5 hours
 


# ** get journals that have topics of my journals, order by works_count/cited_by_count

import ibis
from ibis import _, desc
ibis.options.interactive = True
con = ibis.connect('clickhouse://localhost/litanai')

tbib2 = con.table('bib2')
tsrc = con.table('sources')

tcross2 = (tbib2.filter(_.count_occ > 5)
 .cross_join(tsrc.select('id', 'display_name'))
 .mutate(sim = ngdci(_.journal, _.display_name)))

tcross2_min = (tcross2.group_by(_.journal)
               .aggregate(minsim = _.sim.min())
               .filter(_.minsim < 0.1))

# get the journals i read most
tmyj = (tcross2.inner_join(
    tcross2_min,
    [tcross2.journal == tcross2_min.journal, tcross2.sim == tcross2_min.minsim])
        .select('journal', 'count_occ', 'id', 'display_name'))


con.create_table("bib_myj", schema = tmyj.schema(), overwrite = True)
con.insert('bib_myj', tmyj)



    
xx = get_sim_journals(min_topic_cnt_ttl = 100, min_journal_topic_cnt = 20, min_topics_met = 6)
xx.filter(_.works_count < 19000).aggregate(_.works_count.sum())


 

d_journals.to_csv("~/Dropbox/proj/litanai/res/js2.csv")

d_journals.aggregate(xx = _.works_count.sum())




xx = split_list(d_journals['source_id'].execute().to_list(), 3)

[print(i) for i in xx]


# ** check that ibis can use multiple threads -> yes it can
con_mus = ibis.connect('clickhouse://localhost/music')
tlogs = con_mus.table('logs')

(tlogs.filter(_.time_d < '2007-01-01')
 .group_by('usr', 'song')
 .aggregate(cnd = _.usr.count())
 .order_by('usr', 'song'))


SELECT
    usr,
    song,
    count(*) AS cnt
FROM logs
WHERE time_d < '2007-01-01'
GROUP BY
    usr,
    song

# * convert downloaded files to json.gz

import re
files_pickles = os.listdir(DIR_JOURNAL_PICKLES)
files_already_cvrtd = [re.sub(r'\.json\.gz', '', i) for i in os.listdir(DIR_JOURNAL_GZIP)]

files_to_cvrt = set(files_pickles) - set(files_already_cvrtd)


# '\.json\.gz'

# lmap(convert_dld_file, files_to_cvrt)

# l_entities = pickle_load_entity("S38024979", DIR_JOURNAL_GZIP)
# flatten_works(l_entities)

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

with ProcessPoolExecutor(max_workers=4) as executor:
    executor.map(convert_dld_file, files_to_cvrt)

# * flatten and ingest newly downloaded

import ibis
from ibis import _
import re
ibis.options.interactive = True




l_journals_dld_prep1 = os.listdir(DIR_JOURNAL_GZIP)
l_journals_dld_prep2 = [re.sub(r'\.json\.gz', '', i) for i in l_journals_dld_prep1]
l_journals_dld_prep3 = [i for i in l_journals_dld_prep2 if i[0].lower() == 's']

l_journals_dld = set([s.split('_', 1)[0] for s in l_journals_dld_prep2])
    
# filtered_strings = [s for s in l_journals_dld if s.lower().count('s') > 1]

xx = list(l_journals_dld)[0:5]


con = ibis.connect('clickhouse://localhost/litanai')
tw = con.table("works")
l_journals_ingstd_prep1 = tw.select(_.source_id).distinct().to_pandas()['source_id'].to_list()

l_journals_ingstd = set(lmap(lambda x:re.sub(r'https://openalex.org/', '', x), l_journals_ingstd_prep1))

l_journals_to_ingest = l_journals_dld - l_journals_ingstd
print(len(l_journals_to_ingest))

lmap(lambda x:proc_journal_dispatch(x, "always"), l_journals_to_ingest)

stop



dxj =txj.execute()
len(l_journals_to_dl)

xx = set(l_journals_to_dl) - set(dxj['source_id'].to_list())
# all journals I want are there



# * deduplicate

tw= con.table('works')

qry_dedup = (tw.group_by(_.id, _.source_id)
 .aggregate(nbr_occ = _.id.count())
 .filter(_.nbr_occ > 1)
 .select(_.source_id))

qry_dedup.count()
qry_dedupcn.distinct().count()
             
qry



# * ibis career

ibis.options.interactive = True

search_strings_career = ['%career%', '%life-course%', '%lifecourse%']
search_strings_subject = ['%artist%', '%musician%', '%poet%', '%painter%']
search_strings_outcome = ['%recogni%', '%reputation%', '%consecrat%', '%canoniz%']


qry = (tw.filter(tw.abstract_text.ilike(search_strings_career),
                 tw.abstract_text.ilike(search_strings_subject),
                 tw.abstract_text.ilike(search_strings_outcome))
       .select(_.cited_by_count, _.display_name, _.publication_year, key = _.id, text = _.abstract_text)
       .filter(_.cited_by_count > 5))
       
       

t_cree = gt_cree()

dcree = qry.execute()

view_xl(dcree)

dcree2 = dcree[dcree['cited_by_count'] > 5]

prompt_oai = """
you will read a short text, which generally is the abstract of a scientific article, but it might also be the first few paragraphs. you have to evaluate whether the text is about artist careers, in particular whether it deals with factors leading to successful careers, which might be phrased as success, recognition, reputation, consecration, or canonization. you have to assess a number of things:
- relevance, i.e. whether the text is about artist careers. this should be a decimal number ranging from 0 to 1 based on how relevant the text is for artistic careers
- discipline, which should be your judgement of what discipline the article is from (e.g. sociology, art history, economics)
- time period: which time period the artists are from (if the text is not about artists, return NA)
- methodology: whether the text is quantitative or qualitative (return NA if not applicable)
return these values as a json-dictionary with the keys 'relevance', 'discipline', 'time_period', and 'methodology'

the text follows below: 
"""

import importlib
import litanai
importlib.reload(litanai)
from litanai import litanai, qry_oai_assess, gd_reltexts


dt_reltexts = gd_reltexts(str(ibis.to_sql(qry)))
dx = litanai(dt_reltexts, prompt_oai, qry_oai_assess, "cree_find", False)


# * start with career articles


con = ibis.connect('clickhouse://localhost/litanai')
ibis.options.interactive = True
ibis.options.interactive = False

tsrc = con.table('sources')
tw = con.table('works')

tcree = gt_cree()
twrw = con.table("works_related_works")

# get unique related ones
qry_relw = (twrw.filter(twrw.work_id.isin(tcree.work_id))
 .select(_.related_work_id).distinct())
 
qry_relw.count()
qry_relw_xj = tw.filter(tw.id.isin(qry_relw.related_work_id)) #  get related that exists

# ** get articles that are not in db

# get ids of those that are not in tw
qry_relw_nxj = qry_relw.filter(qry_relw.related_work_id.notin(qry_relw_xj.id))

l_workids = qry_relw_nxj.select('related_work_id').to_pandas()['related_work_id'].to_list()

ls = gd_journals(l_workids)

# query relate journals
qry_rels = tsrc.filter(tsrc.id.isin(ls))

qry_rels.aggregate(_.works_count.sum())

ls2 = qry_rels.filter(
    _.works_count < 1e5).order_by(_.works_count.desc()).select('id').execute()['id'].to_list()




q1 = tcree.inner_join(twrw, "work_id")
q2 = tcree.inner_join(twrw, tcree.work_id == twrw.work_id)
q2.execute()
print(ibis.to_sql(q2))


# * evaluate results

conlite = ibis.connect('sqlite://openai_responses.db')
tresp = conlite.table('cree_find')
tresp.group_by('relevance').aggregate(nbr = _.relevance.count())
tresp.filter(_.relevance > 0.9, _.methodology == 'quantitative').count()
tresp.filter(_.methodology == 'quantitative').count()
view_xl(tresp.filter(_.relevance > 0.5, _.methodology == 'quantitative').execute())

import jutils
importlib.reload(jutils)
from jutils import *

# inspect latest batch (related works)
view_xl((tresp.mutate(row_nbr = ibis.row_number())
 .order_by(desc(_.row_nbr))
 .limit(160)
 .filter(_.relevance > 0.5, _.methodology == 'quantitative')).execute())


# ** qualitative texts
# also look at methodology counts
tresp.group_by('methodology').aggregate(nbr = _.methodology.count())

# look at semi-quantitative texts
view_xl(tresp.filter(_.methodology != 'quantitative', _.methodology.ilike('%quantitative%')).execute())
view_xl(tresp.filter(_.methodology.ilike('%mixed%')).execute())

d_qual = tresp.filter(_.methodology.ilike('%qualitative%'), _.relevance > 0.8)


t_cree_qual = move_tbl_to_ch(d_qual, 'temp_cree_qual')


d_qual2 = (tw.filter(tw.id.isin(t_cree_qual.key))
           .join(t_cree_qual, tw.id == t_cree_qual.key)
           .order_by(_.cited_by_count.desc()))

view_xl(d_qual2.select(_.id, _.display_name, _.abstract_text, _.cited_by_count,
                       _.publication_year, _.discipline).limit(100).execute())




xx = dcree['abstract_text'].to_list()




# * link my bibtex to OA

dt_bibtex = gd_bibtex()

dt_bibtex['title']
