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

DIR_ENTITIES_JSON = "/run/media/johannes/data/litanai/entities_json"

with open(os.path.join(DIR_ENTITIES_JSON, journal_year_id), 'w') as fx:
    json.dump(l_longworks, fx)

