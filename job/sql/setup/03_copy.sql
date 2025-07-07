
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