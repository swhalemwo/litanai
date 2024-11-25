def convert_dld_file (id_short):

    gc.collect()
    print(id_short)

    with open(os.path.join(DIR_JOURNAL_PICKLES, id_short), 'rb') as file:
        l_entities = pickle.load(file)

    pickle_entity(l_entities, id_short, DIR_JOURNAL_GZIP)

    l_entities = 0
    gc.collect()
    

import inspect

class magic_fstring_function:
    def __init__(self, payload):
        self.payload = payload
    def __str__(self):
        vars = inspect.currentframe().f_back.f_globals.copy()
        vars.update(inspect.currentframe().f_back.f_locals)
        return self.payload.format(**vars)

def gt_cree ():

    conch = ibis.connect('clickhouse://localhost/litanai')
    FILE_CAREER_PAPERS
    
    existing_tables = [i for i in conch.tables.__iter__()]

    if "cree" in existing_tables:
        conch.drop_table("cree")

    conch.create_table("cree", schema = ibis.schema({'bibtex_id' : 'string', 'work_id' : 'string'}))

    dt_cree = pd.read_csv(FILE_CAREER_PAPERS)
    conch.insert('cree', dt_cree)

    tcree = conch.table('cree')

    return(tcree)
    
