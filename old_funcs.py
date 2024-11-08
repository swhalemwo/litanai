def convert_dld_file (id_short):

    gc.collect()
    print(id_short)

    with open(os.path.join(DIR_JOURNAL_PICKLES, id_short), 'rb') as file:
        l_entities = pickle.load(file)

    pickle_entity(l_entities, id_short, DIR_JOURNAL_GZIP)

    l_entities = 0
    gc.collect()
    
