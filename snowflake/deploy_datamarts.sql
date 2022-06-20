use role sysadmin;
use database pokemon_database;

-- Task 1.a
create 
or replace view data_marts.type_statistics as (
  with type_stats as (
    select 
      pt.type_name, 
      count(p.id) as pokemon_count, 
      lag(pokemon_count, 1) over(
        order by 
          pokemon_count desc
      ) as previous, 
      lag(pokemon_count, -1) over(
        order by 
          pokemon_count desc, 
          type_name
      ) as next 
    from 
      storage.pokemon p 
      join storage.pokemon_type pt on pt.poke_id = p.id 
    group by 
      pt.type_name 
    order by 
      pokemon_count desc, 
      type_name
  ) 
  select 
    type_name, 
    pokemon_count, 
    case when (previous - pokemon_count) is null then 0 else (previous - pokemon_count) end as change_from_next_higher, 
    case when (pokemon_count - next) is null then 0 else (pokemon_count - next) end as change_from_next_lower 
  from 
    type_stats 
  order by 
    pokemon_count desc, 
    type_name
);

-- Task 1.b
create 
or replace view data_marts.move_statistics as (
  with move_stats as (
    select 
      pm.move_name, 
      count(p.id) as pokemon_count, 
      lag(pokemon_count, 1) over(
        order by 
          pokemon_count desc, 
          move_name
      ) as previous, 
      lag(pokemon_count, -1) over(
        order by 
          pokemon_count desc, 
          move_name
      ) as next 
    from 
      storage.pokemon p 
      join storage.pokemon_move pm on pm.poke_id = p.id 
    group by 
      pm.move_name 
    order by 
      pokemon_count desc, 
      move_name
  ) 
  select 
    move_name, 
    pokemon_count, 
    case when (previous - pokemon_count) is null then 0 
      else (previous - pokemon_count) end as change_from_next_higher, 
    case when (pokemon_count - next) is null then 0 
      else (pokemon_count - next) end as change_from_next_lower 
  from 
    move_stats 
  order by 
    pokemon_count desc, 
    move_name
);

-- Task 1.c
create view data_marts.pokemon_stat_rating (name, strength) as (
  select 
    p.name, 
    sum(value) 
  from 
    storage.pokemon_stat ps 
    join storage.pokemon p on p.id = ps.poke_id 
  group by 
    p.name 
  order by 
    sum(value) desc
);

-- Task 1.d
create 
or replace view data_marts.generation_type_rating (generation, type, quantity) as (
  select 
    case when grouping_id(p.generation) = 1 then 'All generations' else p.generation end, 
    case when grouping_id(pt.type_name) = 1 then 'All types' else pt.type_name end, 
    count(p.id) 
  from 
    storage.pokemon p 
    join storage.pokemon_type pt on pt.poke_id = p.id 
  group by 
    cube(p.generation, pt.type_name)
);
