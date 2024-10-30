PROJ_DIR = "/home/johannes/Dropbox/proj/litanai/"


query_reltext = "".join(["select key, length(text) AS len, ", 
                     "(length(text) - length(replace(text, 'private museum', ''))) /",
                     "length('private museum') AS n_occur, text ", 
                     " from littext where text LIKE '%private museum%'",
                     " AND n_occur > 10",
                     " order by n_occur DESC"])


prompt = """you will read a long text. the text is in some way about private art museums, a new form of museums started by wealthy collectors. you have to find every instance in this text about how private art museums have (or have not) an effect on the arts, for example that the artists after being exhibted experience a boost to their career, increase their chances of canonization or consecration, are more likely to be raise higher prices at auctions or are more likely to be exhibited by other museums or institutions. Any impact that private museums leave in the field of artistic production."""



# single query
jj = qry_oai("Brown_2019_private.pdf", prompt, dt_reltexts.iloc[0]['text'])



# ** nonprofit survival


query_nposurv = """select key, len, text, n_surv + n_closing + n_closure + n_cox as surv_ttl, n_np1 + n_np2 as n_np from (
SELECT key, length(text) as len, text, 
(LENGTH(text) - LENGTH(REPLACE(text, 'surviv', ''))) / LENGTH('surviv') AS n_surv,
(LENGTH(text) - LENGTH(REPLACE(text, 'closing', ''))) / LENGTH('closing') AS n_closing,
(LENGTH(text) - LENGTH(REPLACE(text, 'closure', ''))) / LENGTH('closure') AS n_closure,
(LENGTH(text) - LENGTH(REPLACE(text, 'cox', ''))) / LENGTH('cox') AS n_cox,
(LENGTH(text) - LENGTH(REPLACE(text, 'non-profit', ''))) / LENGTH('non-profit') AS n_np1,
(LENGTH(text) - LENGTH(REPLACE(text, 'nonprofit', ''))) / LENGTH('nonprofit') AS n_np2
FROM littext)
where surv_ttl > 8 and n_np > 8"""

dt_rt_npo = gd_reltexts(query_nposurv)

dt_rt_npo[dt_rt_npo['tokens'] < 100000]


prompt_npo = gs_oai_prompt(
    "factors associated with non-profit closure",
    """you will read a text long which is some way related to non-profit organizations. It very likely is in some way about non-profit organizations closing (or surviving). Your task is to identify every instance where the text talks about factors associated with closings. These can be properties of the non-profit organization itself, the environent it is situated in, or really anything. """)

litanai(query_reltext = query_nposurv, query_oai = prompt_npo, proj_name = "npo_surv", head =False)

# ** role of individuals


prompt_vid = gs_oai_prompt(
    'the role of individuals in museum closures',
    """you will read a scientific article which in some way if related to the closure of non-profit organizations. Your task is to identify every instance where the texts talks about the role that individual people play in the closing of non-profit organizations. you should look for factors on the level of the individual person, such as exhaustion, demotivation or conflict within the organization. only report instances where individual-level factors are measured directly.""")
# Do NOT try to infer individual-level factors from organizational ones, such as organizational age or expenditure; 
print(prompt_vid)

litanai(query_reltext = query_nposurv, prompt_oai = prompt_vid, proj_name = "surv_vid", head = False)

xx = qry_oai("Fernandez", prompt_vid, dt_rt_npo.iloc[19]['text'])
xx.to_csv(f"{PROJ_DIR}/res/res_vid_test.csv")


query_coords = """select key, n_coords, len, text,  n_surv + n_closing + n_closure + n_cox as surv_ttl, n_np1 + n_np2 as n_np from (
SELECT key, length(text) as len, text, 
(LENGTH(text) - LENGTH(REPLACE(text, 'surviv', ''))) / LENGTH('surviv') AS n_surv,
(LENGTH(text) - LENGTH(REPLACE(text, 'closing', ''))) / LENGTH('closing') AS n_closing,
(LENGTH(text) - LENGTH(REPLACE(text, 'closure', ''))) / LENGTH('closure') AS n_closure,
(LENGTH(text) - LENGTH(REPLACE(text, 'cox', ''))) / LENGTH('cox') AS n_cox,
(LENGTH(text) - LENGTH(REPLACE(text, 'non-profit', ''))) / LENGTH('non-profit') AS n_np1,
(LENGTH(text) - LENGTH(REPLACE(text, 'nonprofit', ''))) / LENGTH('nonprofit') AS n_np2,
(LENGTH(text) - LENGTH(REPLACE(text, 'coordinates', ''))) / LENGTH('coordinates') AS n_coords
FROM littext)
where surv_ttl > 8 and n_np > 8"""

dt_rt_coords = gd_reltexts(query_coords)
