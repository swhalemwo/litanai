
-- DROP TABLE IF EXISTS litanai.works;

CREATE TABLE IF NOT EXISTS litanai.works (
  id text, 
  doi text,
  title text,
  display_name text,
  publication_year integer,
  publication_date text,
  type text,
  cited_by_count integer,
  is_retracted boolean,
  is_paratext boolean,
  cited_by_api_url text,
  abstract_text text,
  language text,
  volume text,
  issue text,
  first_page text,
  last_page text,
  mag bigint,
  pmid text,
  pmcid text,
  is_oa BOOLEAN,
  oa_status text,
  oa_url text,
  any_repository_has_fulltext BOOLEAN,
  source_id text,
  landing_page_url text,
  pdf_url text,
  version text,
  license text	
) ENGINE = MergeTree()
  ORDER BY publication_year;

-- DROP TABLE IF EXISTS litanai.works_related_works;

CREATE TABLE IF NOT EXISTS litanai.works_related_works (
  work_id text,
  related_work_id text
) ENGINE = MergeTree()
  ORDER BY related_work_id;


-- DROP TABLE IF EXISTS litanai.works_referenced_works;

CREATE TABLE IF NOT EXISTS litanai.works_referenced_works (
  work_id text,
  referenced_work_id text
) ENGINE = MergeTree()
  ORDER BY referenced_work_id;




-- INSERT INTO litanai.works FROM
-- INFILE '/home/johannes/Dropbox/proj/litanai/oa_csv_files/works.csv.gz'
-- COMPRESSION 'gzip' FORMAT CSV;

-- select title from works LIMIT 10

DROP TABLE if EXISTS litanai.sources;

CREATE TABLE litanai.sources (
  id text NOT NULL,
  issn_l text,
  display_name text,
  publisher text,
  works_count integer,
  cited_by_count integer,
  is_oa boolean,
  is_in_doaj boolean,
  homepage_url text,
  works_api_url text,
  updated_date DateTime64
) ENGINE = MergeTree()
  ORDER BY id;


DROP TABLE if EXISTS litanai.sources_counts_by_year;

CREATE TABLE litanai.sources_counts_by_year (
  source_id text NOT NULL,
  year integer NOT NULL,
  works_count integer,
  cited_by_count integer,
  oa_works_count INTEGER
) ENGINE = MergeTree()
  ORDER BY source_id;


--
-- Name: sources_ids; Type: TABLE; Schema: openalex; Owner: -
--

DROP TABLE if EXISTS litanai.sources_ids;

CREATE TABLE litanai.sources_ids (
  source_id text,
  openalex text,
  issn_l text,
  mag bigint,
  wikidata text,
  fatcat text
) ENGINE = MergeTree()
  ORDER BY source_id;

