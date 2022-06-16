use role sysadmin;

-- TABLES (see readme for schema)

create table storage.pokemon(
    id integer primary key,
    name varchar,
    generation varchar);
    
create table storage.pokemon_move(
    id integer primary key autoincrement,
    poke_id int  references storage.pokemon(id),
    move_name varchar);
    
create table storage.pokemon_type(
    id integer primary key autoincrement,
    poke_id int references storage.pokemon(id),
    type_name varchar);
    
create or replace table storage.stat(
    id integer primary key autoincrement,
    stat_name varchar);
    
create table storage.pokemon_stat(
    id integer primary key autoincrement,
    poke_id int  references storage.pokemon(id),
    stat_id int  references storage.stat(id),
    value int);
  
-- TASKS

create or replace task staging.load_to_pokemon 
warehouse = tasks_wh schedule = '5 minutes' as 
insert into storage.pokemon(id, name, generation)
(with pokemon as
    (select json_data:id as id,
    json_data:name as name,
    json_data:species:name as species
    from staging.stream_pokemon),
generations as
    (select
    json_data:id as id,
    value:name as generation_name
    from staging.stg_generations,
    lateral flatten(input => json_data:names)
    where value:language:name = 'en'),
generation_pokemon as
    (select 
    json_data:id as id,
    value:name as pokemon_species
    from staging.stg_generations,
    lateral flatten(input => json_data:pokemon_species))

select p.id, p.name, g.generation_name
from pokemon p
left join generation_pokemon gp on p.species = gp.pokemon_species
left join generations g on g.id = gp.id);


create or replace task staging.load_to_pokemon_move
warehouse = tasks_wh schedule = '5 minutes' as 
insert into storage.pokemon_move(poke_id, move_name)
select json_data:id as poke_id,
value:move:name as move_name
from staging.stream_pokemon_move,
lateral flatten(input => json_data:moves);


create or replace task staging.load_to_pokemon_type
warehouse = tasks_wh schedule = '5 minutes' as 
insert into storage.pokemon_type(poke_id, type_name)
select
json_data:id as id,
value:type:name as type_name
from staging.stream_pokemon_type,
lateral flatten(input => json_data:types);

create or replace task staging.load_to_stat
warehouse = tasks_wh schedule = '5 minutes' as 
insert into storage.stat(stat_name)
select distinct value:stat:name  as stat_name
from staging.stream_stat,
lateral flatten(input => json_data:stats);

create or replace task staging.load_to_pokemon_stat
warehouse = tasks_wh schedule = '5 minutes' as 
insert into storage.pokemon_stat(poke_id, stat_id, value)
(with named_stat_facts as
(select json_data:id as poke_id,
value:stat:name as stat_name,
value:base_stat as value
from staging.stream_pokemon_stat,
lateral flatten(input => json_data:stats))
 
select nsf.poke_id, s.id, nsf.value
from named_stat_facts nsf
join  storage.stat s on s.stat_name = nsf.stat_name);

execute task staging.load_to_pokemon;
execute task staging.load_to_pokemon_move;
execute task staging.load_to_pokemon_type;
execute task staging.load_to_stat;
execute task staging.load_to_pokemon_stat;
