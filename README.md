# Pokemon_DWH
Data warehouse for loading, storing, and analyzing data obtained through Pokeapi.
## Structure
Data is fetched from Pokeapi and saved on AWS S3 bucket using Amazon Managed Workflows for Apache Airflow (MWAA). All nessecary code for that is stored in the airflow/ folder. There are two main DAGs that do most of the work - dag_verzun_load searches for all available esources for generations and pokemon in PokeAPI and loads them into a specified location on S3 bucket. dag_verzun_generations checks for number of generations every day (or whenever asked) and logs the changes (or the absence of such). dag_verzun_cleanup deletes all the files from the working directory and is supposed to be used for debugging.

Data is then loaded on Snowflake via snowpipe. Scripts for deploying the warehouse in Snowflake can be found in the snowflake/ folder. Snowpipe automatically copies all data into 2 tables - stg_pokemon and stg_generations, from which data is loaded into the actual storage schema with the help of streams and tasks. Data marts are created as views based on the tables from the storage schema.

## Schema
A snowflake schema was used to represent the data in the data warehouse. Below is the Storage schema:

![Snowflake Schema](https://i.ibb.co/Wgnpqjw/snowflake-schema-final-pages-to-jpg-0002.jpg)

## Data Marts
There are 4 data marts representing different statistics according to the specified tasks.

a. The data_marts.type_statistics view represents a ranking of types by quantity of pokemon belonging to each type:

![Type Statistics](https://i.ibb.co/wR53Kxw/a.jpg)

b. The data_marts.move_statistics view is similar to the previous one but presents a ranking of moves:

![Move Statistics](https://i.ibb.co/ZSzS5Tw/b.jpg)

c. Ranking of pokemon by sum of base stats can be found in the data_marts.pokemon_stat_rating view:

![Strength Ranking](https://i.ibb.co/ZXYBH1R/c.jpg)

d. The data_marts.generation_type_rating view shows the quantity of pokemon by each generation and type, as well as totals:

![Generation and Type Statistics](https://i.ibb.co/M1g79Df/d.jpg)

## Potential issues and further improvements
1) Memory issues may appear if data gets too big. The \_load_from_resources function used to partition data authomatically but I removed this feature since it did not prove useful. 
2) The Snowflake task staging.load_to_pokemon_stat may not work correctly because it depends on the previous task being completed. However, this issue may only appear when data is loaded for the first time unless new stat types are introduced. 
3) There is no checking for uniqueness currently implemented.
4) Getting ids from url may become a problem if the API changes.
5) Deltas in data marts are not always comprehensive when the delta is 0 (see datamart b).
6) Cleanup DAG deletes the logging file too. It can be modified to do regular cleanups rather than delete the working directory altogether but it is currently outside of the scope of the project and not really needed.
7) No notifications for logging.WARNING. They mostly require additional dependencies.
8) Annoying tabs in logging.
