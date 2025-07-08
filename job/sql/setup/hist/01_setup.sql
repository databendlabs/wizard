CREATE OR REPLACE STAGE wizardbend
    URL = 's3://wizardbend/';

CREATE OR REPLACE DATABASE imdb_hist;
USE imdb_hist;