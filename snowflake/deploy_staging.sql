-- SETUP
use role sysadmin;

create 
or replace database pokemon_database;

-- WAREHOUSES
create 
or replace warehouse deployment_wh warehouse_size = XSMALL max_cluster_count = 1 min_cluster_count = 1;

create 
or replace warehouse tasks_wh warehouse_size = XSMALL max_cluster_count = 1 min_cluster_count = 1;

use warehouse deployment_wh;
-- SCHEMAS
create schema staging;
create schema storage;
create schema data_marts;

--TABLES
create 
or replace table staging.stg_generations(json_data variant);

create 
or replace table staging.stg_pokemon(json_data variant);
-- STREAMS 
-- One stream called stream_{tablename} for each table
create 
or replace stream staging.stream_pokemon on table staging.stg_pokemon;

create 
or replace stream staging.stream_pokemon_type on table staging.stg_pokemon;

create 
or replace stream staging.stream_pokemon_move on table staging.stg_pokemon;

create 
or replace stream staging.stream_stat on table staging.stg_pokemon;

create 
or replace stream staging.stream_pokemon_stat on table staging.stg_pokemon;

-- STAGE
create stage staging.SF url = 's3://de-school-snowflake/' credentials =(
  aws_key_id = 'Maybe later' aws_secret_key = 'No secret key here'
);

-- PIPES
create 
or replace pipe staging.generations_pipe auto_ingest = true as copy into staging.stg_generations(json_data) 
from 
  (
    select 
      $1 
    from 
      @staging.SF/snowpipe/Verzun
  ) pattern = '.*generation.json' file_format = (
    type = 'JSON' strip_outer_array = true
  );
  
alter pipe staging.generations_pipe refresh;

create 
or replace pipe staging.pokemon_pipe auto_ingest = true as copy into staging.stg_pokemon(json_data) 
from 
  (
    select 
      $1 
    from 
      @staging.SF/snowpipe/Verzun
  ) pattern = '.*pokemon.json' file_format = (
    type = 'JSON' strip_outer_array = true
  );
  
alter pipe staging.pokemon_pipe refresh;
  