-- Create IMDB database and tables
CREATE OR REPLACE DATABASE imdb;

USE imdb;

CREATE TABLE aka_name (
    id integer NOT NULL,
    person_id integer NOT NULL,
    name text NOT NULL,
    imdb_index varchar,
    name_pcode_cf varchar,
    name_pcode_nf varchar,
    surname_pcode varchar,
    md5sum varchar
);

CREATE TABLE aka_title (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    title text NOT NULL,
    imdb_index varchar,
    kind_id integer NOT NULL,
    production_year integer,
    phonetic_code varchar,
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    note text,
    md5sum varchar
);

CREATE TABLE cast_info (
    id integer NOT NULL,
    person_id integer NOT NULL,
    movie_id integer NOT NULL,
    person_role_id integer,
    note text,
    nr_order integer,
    role_id integer NOT NULL
);

CREATE TABLE char_name (
    id integer NOT NULL,
    name text NOT NULL,
    imdb_index varchar,
    imdb_id integer,
    name_pcode_nf varchar,
    surname_pcode varchar,
    md5sum varchar
);

CREATE TABLE comp_cast_type (
    id integer NOT NULL,
    kind varchar NOT NULL
);

CREATE TABLE company_name (
    id integer NOT NULL,
    name text NOT NULL,
    country_code varchar,
    imdb_id integer,
    name_pcode_nf varchar,
    name_pcode_sf varchar,
    md5sum varchar
);

CREATE TABLE company_type (
    id integer NOT NULL,
    kind varchar NOT NULL
);

CREATE TABLE complete_cast (
    id integer NOT NULL,
    movie_id integer,
    subject_id integer NOT NULL,
    status_id integer NOT NULL
);

CREATE TABLE info_type (
    id integer NOT NULL,
    info varchar NOT NULL
);

CREATE TABLE keyword (
    id integer NOT NULL,
    keyword text NOT NULL,
    phonetic_code varchar
);

CREATE TABLE kind_type (
    id integer NOT NULL,
    kind varchar NOT NULL
);

CREATE TABLE link_type (
    id integer NOT NULL,
    link varchar NOT NULL
);

CREATE TABLE movie_companies (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    company_id integer NOT NULL,
    company_type_id integer NOT NULL,
    note text
);

CREATE TABLE movie_info (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info text NOT NULL,
    note text
);

CREATE TABLE movie_info_idx (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info text NOT NULL,
    note text
);

CREATE TABLE movie_keyword (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    keyword_id integer NOT NULL
);

CREATE TABLE movie_link (
    id integer NOT NULL,
    movie_id integer NOT NULL,
    linked_movie_id integer NOT NULL,
    link_type_id integer NOT NULL
);

CREATE TABLE name (
    id integer NOT NULL,
    name text NOT NULL,
    imdb_index varchar,
    imdb_id integer,
    gender varchar,
    name_pcode_cf varchar,
    name_pcode_nf varchar,
    surname_pcode varchar,
    md5sum varchar
);

CREATE TABLE person_info (
    id integer NOT NULL,
    person_id integer NOT NULL,
    info_type_id integer NOT NULL,
    info text NOT NULL,
    note text
);

CREATE TABLE role_type (
    id integer NOT NULL,
    role varchar NOT NULL
);

CREATE TABLE title (
    id integer NOT NULL,
    title text NOT NULL,
    imdb_index varchar,
    kind_id integer NOT NULL,
    production_year integer,
    imdb_id integer,
    phonetic_code varchar,
    episode_of_id integer,
    season_nr integer,
    episode_nr integer,
    series_years varchar,
    md5sum varchar
);

-- Create stage pointing to public AWS bucket
CREATE OR REPLACE STAGE wizardbend
    URL = 's3://wizardbend/';

-- Load data from the public AWS bucket
COPY INTO aka_name
    FROM @wizardbend/job/aka_name.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO aka_title
    FROM @wizardbend/job/aka_title.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO cast_info
    FROM @wizardbend/job/cast_info.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO char_name
    FROM @wizardbend/job/char_name.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO comp_cast_type
    FROM @wizardbend/job/comp_cast_type.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO company_name
    FROM @wizardbend/job/company_name.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO company_type
    FROM @wizardbend/job/company_type.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO complete_cast
    FROM @wizardbend/job/complete_cast.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO info_type
    FROM @wizardbend/job/info_type.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO keyword
    FROM @wizardbend/job/keyword.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO kind_type
    FROM @wizardbend/job/kind_type.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO link_type
    FROM @wizardbend/job/link_type.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO movie_companies
    FROM @wizardbend/job/movie_companies.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO movie_info
    FROM @wizardbend/job/movie_info.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO movie_info_idx
    FROM @wizardbend/job/movie_info_idx.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO movie_keyword
    FROM @wizardbend/job/movie_keyword.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO movie_link
    FROM @wizardbend/job/movie_link.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO name
    FROM @wizardbend/job/name.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO person_info
    FROM @wizardbend/job/person_info.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO role_type
    FROM @wizardbend/job/role_type.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');

COPY INTO title
    FROM @wizardbend/job/title.csv
    FILE_FORMAT = (TYPE = CSV, ESCAPE='\\');