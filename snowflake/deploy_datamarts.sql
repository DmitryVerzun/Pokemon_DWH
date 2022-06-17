use role sysadmin;
use database pokemon_database;


create view data_marts.pokemon_stat_rating (name, strength) as
(select p.name, sum(value) 
from storage.pokemon_stat ps
join storage.pokemon p on p.id = ps.poke_id
group by p.name
order by sum(value) desc);

-- need to name grand totals, don't forget
create or replace view data_marts.generation_type_rating (generation, type, quantity) as
(select 
 case when grouping_id(p.generation) = 1 then 'All generations' else p.generation end,
 case when grouping_id(pt.type_name) = 1 then 'All types' else pt.type_name end,
 count(p.id)
from storage.pokemon p
join storage.pokemon_type pt on pt.poke_id = p.id
group by cube(p.generation, pt.type_name));

select * from data_marts.generation_type_rating;

create or replace view data_marts.move_statistics as
(with move_stats as
(select pm.move_name, count(p.id) as pokemon_count,
 lag(pokemon_count, 1) over(order by pokemon_count desc) as previous,
 lag(pokemon_count, -1) over(order by pokemon_count desc) as next
from storage.pokemon p
join storage.pokemon_move pm on pm.poke_id = p.id
group by pm.move_name
order by pokemon_count desc)

select move_name, pokemon_count,
(previous - pokemon_count) as change_from_next_higher, 
(pokemon_count - next) as change_from_next_lower
from move_stats order by pokemon_count desc);

create or replace view data_marts.type_statistics as
(with type_stats as
(select pt.type_name, count(p.id) as pokemon_count,
 lag(pokemon_count, 1) over(order by pokemon_count desc) as previous,
 lag(pokemon_count, -1) over(order by pokemon_count desc) as next
from storage.pokemon p
join storage.pokemon_type pt on pt.poke_id = p.id
group by pt.type_name
order by pokemon_count desc)

select type_name, pokemon_count,
(previous - pokemon_count) as change_from_next_higher, 
(pokemon_count - next) as change_from_next_lower
from type_stats order by pokemon_count desc);

select * from data_marts.type_statistics;
