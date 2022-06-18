# Pokemon_DWH
Data warehouse for loading, storing, and analyzing data obtained through Pokeapi.
## Structure
Data is fetched from Pokeapi and saved on AWS S3 bucket using Amazon Managed Workflows for Apache Airflow (MWAA). All nessecary code for that is stored in the airflow/ folder.

Data is then loaded on Snowflake via snowpipe. Scripts for deploying the warehouse in Snowflake can be found in the snowflake/ folder
## Schema
A snowflake schema was used to represent the data in the data warehouse. Below is the Storage schema. 
## Data Marts
There are 4 data marts representing different statistics.

a. The data_marts.type_statistics view represents a ranking of types by quantity of pokemon belonging to each type.
![Type Statistics](https://i.ibb.co/wR53Kxw/a.jpg)

b. The data_marts.move_statistics view is similar to the previous one but presents a ranking of moves.

c. Ranking of pokemon by sum of base stats can be found in the data_marts.pokemon_stat_rating view.

d. The data_marts.generation_type_rating view shows the quantity of pokemon by each generation and type, as well as totals.

## Potential issues
1) Memory issues may appear if data gets too big. The \_load_from_resources function used to partition data authomatically but i removed this feature since it did not prove useful. 
2) Task staging.load_to_pokemon_stat may not work correctly because it depends on previous task. However, this issue only appears when data is loaded for the first time since scratch unless new stat types are introduced. 
3) There is no checking for uniqueness currently implemented.
4) Getting ids from url may become a problem if the API changes.
5) New threads are created for each API request. This may be wasteful but didn't cause any problems.
6) Deltas in data marts are not always comprehensive when difference is 0 (see datamart b).