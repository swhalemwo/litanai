# uri = 'clickhouse://default:@localhost/litanai'
import ibis
from ibis import _, desc
ibis.options.interactive = True


conch = ibis.connect('clickhouse://localhost/litanai')
tw = conch.table('works')
tsrc = conch.table('sources')

conlite = ibis.connect('sqlite://openai_responses.db')
tlit = conch.table('littext')




# metadata.tables.keys()

DIR_CSV = '/home/johannes/Dropbox/proj/litanai/oa_csv_files/'
DIR_JOURNAL_PICKLES = "/run/media/johannes/data/litanai/journals/"
DIR_JOURNAL_GZIP = "/run/media/johannes/data/litanai/gzip/"
DIR_LIT = "/home/johannes/Dropbox/readings/"

DIR_PDF = "/run/media/johannes/data/litanai/pdf/"
FILE_CAREER_PAPERS = "~/Dropbox/phd/papers/infl/lit/lit_infl.csv"
PROJ_DIR = "/home/johannes/Dropbox/proj/litanai/"

DBNAME = "litanai"

