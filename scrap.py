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
