import pyalex
from pyalex import Works, Authors, Sources, Institutions, Topics, Publishers, Funders, config, invert_abstract

from flatten_openalex_jsonl import flatten_works, init_dict_writer

config.email
config.email = "johannes.ae@hotmail.de"
config.max_retries = 0
config.retry_backoff_factor = 0.1
config.retry_http_codes = [429, 500, 503]


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

[os.remove(CSV_DIR + file) for file in os.listdir(CSV_DIR)]

flatten_works(l_w_poetics_flat)


CSV_DIR = '/home/johannes/Dropbox/proj/litanai/oa_csv_files/'
