# Pokemon_DWH
Data warehouse for loading, storing, and analyzing data obtained through Pokeapi.
## Structure
Data is fetched from Pokeapi and saved on AWS S3 bucket using Amazon Managed Workflows for Apache Airflow (MWAA). All nessecary code for that is stored in the airflow/ folder.

Data is then loaded on Snowflake via snowpipe. Scripts for deploying the warehouse in Snowflake can be found in the snowflake/ folder
## Schema
A snowflake schema was used to represent the data in the data warehouse. Below is the Storage schema. 
![Snowflake Schema](https://i.ibb.co/Wgnpqjw/snowflake-schema-final-pages-to-jpg-0002.jpg)

## Data Marts
There are 4 data marts representing different statistics.

a. The data_marts.type_statistics view represents a ranking of types by quantity of pokemon belonging to each type.
![Type Statistics](https://i.ibb.co/wR53Kxw/a.jpg)

b. The data_marts.move_statistics view is similar to the previous one but presents a ranking of moves.
![Move Statistics](https://i.ibb.co/ZSzS5Tw/b.jpg)

c. Ranking of pokemon by sum of base stats can be found in the data_marts.pokemon_stat_rating view.
![Strength Ranking](https://i.ibb.co/ZXYBH1R/c.jpg)

d. The data_marts.generation_type_rating view shows the quantity of pokemon by each generation and type, as well as totals.
![Generation and Type Statistics](https://i.ibb.co/M1g79Df/d.jpg)

## Potential issues and further improvements
1) Memory issues may appear if data gets too big. The \_load_from_resources function used to partition data authomatically but I removed this feature since it did not prove useful. 
2) The Snowflake task staging.load_to_pokemon_stat may not work correctly because it depends on the previous task being completed. However, this issue may only appear when data is loaded for the first time unless new stat types are introduced. 
3) There is no checking for uniqueness currently implemented.
4) Getting ids from url may become a problem if the API changes.
5) New threads are created for each API request. This may be wasteful but didn't cause any problems.
6) Deltas in data marts are not always comprehensive when the delta is 0 (see datamart b).
7) No DAG for cleanup 
8) No notifications for logging.WARNING. They mostly require additional dependencies.