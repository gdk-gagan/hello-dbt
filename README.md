# hello-dbt
Learn dbt by implementing a project based on NY taxi open dataset in Google Bigquery. It includes creating tests, documentation and creating jobs for production deployment.

### Pre-requisites
1. You need a Google Cloud account and the google cloud sdk (download here). Verify your installation by running - 
   ```bash 
   gcloud version
   ```
3. Upload required data to GCS using `scripts/upload_to_gcs.py` [Code inspired from DE-Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_3_data_warehouse/extras)
4. Ingest data from GCS to BigQuery using `scripts/upload_gcs_to_bq.py`
5. After running the scripts above, you should have the following schema in your BigQeury project.
   - your-google-cloud-project-name
     - prod
     - ny_taxi
       - yellow_taxi
       - green_taxi
6. Also, create a new conda enviroment, say `dbt`.

### Setup
Setting up dbt locally with bigquery
- Enable bigquery APIs
- create a service account and give bigquery admin permissions
- install dbt-bigquery [dbt install](https://docs.getdbt.com/docs/core/pip-install)
- setup a dbt project using dbt init - it requires yours google project-id and the path to your service credentials json.
- test your connection using `dbt debug`
- if you run into errors, make sure dbt can find profile.yml (check if ~/.dbt/profile.yml exists) and that you are using the correct profile name in your dbt_project.yml

Dry run - 
- Go through dbt_project.yml. It sets global configs for your project. Verify the dbt project name, profile, etc.
- Note the setup include sample models in the `models` dir. 
- If you run, `dbt run`, it will create a sample table and a view called `my_first_dbt_model` and `my_second_dbt_model` respectively in your BigQuery schema.
- Try `dbt test`. It runs the sample tests for your sample models.

### About the NY-taxi Analytics Engineering Project
We have publically available raw trip data for taxi rides in New York (available here) that we ingest in our data warehouse (Bigquery for this project).
We would like to analyze this data, define metrics and make it available for business reporting.Some questions we would like to answer - 
- Revenue analysis
  - Monthly revenue (total and breakdown by zone, location)
- Location analysis
- Trip analysis
  - Peak hours

#### What is a dbt model?

The data is available for each month and service type (yellow, green etc). In order to combine these data, we need to map the columns s.t they have same names and types. 
The combined dataset will form the base dataset upon which all analyses will be based. 
This dataset would need to updated with the most recent data.
This step for cleaning the source tables will be handled in `staging` folder.
So the stage folder contains sql queries that clean the source tables as required and save them as views or tables. Each table or view is saved in separate file.


*A dbt model is basically sql file with a select statement.*

*Running the model creates a table/view of the same name as the file based on the select statement.*

`staging` folder is conventionally used to store intermediate models to be used for final models. These intermediate models are typically data cleaning steps or any logic that needs to applied globally across all use-cases for that data. Stage models are typically saved as views so that we always have the latest data without running table update queries.

stg_green_trips.sql: creates a view with the name of the file
stg_yellow_trips.sql:

`core` folder keeps the final models that answer specific questions or had a specific use-case. We use kimball dimensional modelling creating fact `fact_` and dimension `dim_` models.
fact_trips.sql:
dim_zones.sql:
dim_monthly_zone_revenue.sql:


Each folder under models has its own schema.yml file that maps the referenced tables in the model to actual table names in the DWH.

### Step by step guide
- install dbt (local or cloud)
- create a dbt project using `dbt init`
- it takes input for project name and your dwh environment and stores it under ~/.dbt/profiles.yml. This file contains your environment configurations.
- checkout `dbt_project.yml`. this is your global config file. Give a project name and set a profile name (defines the enviroment/database that your dbt project will run in)
  ```bash
        name: <my-dbt-project>
        profile: <env-name>
        
        models:
                <my-dbt-project>:
  ```
 - `target` folder will contain the compiled code and it already included in `.gitingnore`

- About Jinja - blocks defined by `{{ }}` or `{% %}` or `{# this is a comment block  #}`
- A dbt model is a sql file with a select statement that either create a table or a view depening on the given materialization strategy.
- Materialisation strategy is defined at the beginning of the model using a jinja block as below. It can be a table, view, incremental (process updates only), or ephemeral (is derived model with a CTE defined in another file).
  ```sql
    {{ config(materialized='view') }}
  ```
- FROM clause
  - Source
    - We can use the `source` macro to call source tables in our DWH in the `from` clause.
     ```sql 
     select * from {{ source('staging', 'yellow_trip_data') }}
     where vendor_id is not null
     ```
    - `yellow_trip_data` is a variable defined in the schema.yml and maps to the actual table (`yellow_taxi`) in the DWH
  - What is a dbt Seed?
    - Another way to load data into your DWH is by directly uploading a csv file. This is called a seed in dbt.
    - Usually a lookup file that doesn't change very often
    - In this case, the file lives inside the repository under version control.
    - Seeds are referenced in the select query using `ref` macro.
     ```sql
        select zone, zone_id
        from {{ ref('taxi_zone_lookup') }} 
    ```
        The `taxi_zone_lookup` is the name of the csv file.
        - Run using 
      ```bash
      dbt seed # for all files in the seed folder
      dbt seed -f filename # for a particular file 
      ```
      Running the command creates a table or view in the DWH.
    - `ref` can be used to reference any table or view created by a dbt model or dbt seeds. We just need to write a single table name and it automatically resolves the full name and dependencies based on the environment in which the code is run.
    - NOTE: we can also defined source freshness in our schema to check how old is the latest data against the threshold defined in the freshness config.
    - Create two folders under `models` - 
      - `staging` - to store raw data with some type casting and column renaming.
        - Create a new file called stg_yellow_trips.sql
        - Start with the config block.
        - define the source used in select in schema.yml. the name of the source doesn't need to match the table name in the dwh. this mapping is managed in the yaml file.
        - a source is mapped to a database and a schema and can contain one or more tables. 
        - You can define freshness at the table or source level.
      - `core` - models that'll be exposed to our stakeholders through BI tools. Ideally all core models should be tables, as they are being used downstream, tables would be more efficient.
      - Run the model using 
        ```bash
        dbt run # run all models under models directory
        dbt run -m <file.sql> # run a particular model
        dbt run --select <model-name> # model name is same as filename without the extension.
        dbt run -f <file.sql> # using filename
        ```
        Review the results. If a model based on another model fails, the second model is *skipped*.
- What are dbt macros?
  - similar to functions but written in jinja, like source, ref, config
  - dbt provides several pre-defined macros.
  - useful to define transformations that are used by multiple models.
  - we can have custom macros as well
  - in dbt, macros return compiled code instead of a result
  - defined in separate files under `macros` folder using a combination of sql and jinja.
  ```sql
    {% macro function_name(params) -%}

        case {{payment_type}}
            when 1 then 'cash'
            when 2 then 'credit'
            when 3 then 'debit'
        end

   {%- endmacro %}
  ```
To call the macro
```sql
{{ get_payment_type_description('<column_name>') }} as payment_type_description
```

  - we can use macros from other projects using *packages*.
  - these packages are basically standalone dbt projects with models and macros.
  - a list of packages can be found in the dbt package hub.
  - to use a model or a macro from another project, 
    - create a `packages.yml` in the main directory of your project
    - define the packages you want to use
      - eg, to use db_utils packages
        ```sql
        packages:
        - package: dbt-labs/dbt_utils
            version: 0.8.0
        ```
    - To install the packages, run
        ```bash
            dbt deps
        ```
        This creates a folder `db_utils` under `dbt_packages` that has all the code.
    - calling the macros from the model. For example, `dbt_utils.surrogate_key` basically implements an md5 function on the given parameters and returns a unique key.
        ```sql
        {{ dbt_utils.surrogate_key(['vendorid', 'lpep_pickup_datetime']) }} as tripid,
        ```
- variables
    - defining values to be used across the project
    - Usage - with `var` macro. we can also specify a default value.
    - ```sql
        var('is_test_run', default=true)
      ```
    - can be defined in two ways
      1. as global variable who values are defined in the project.yml file 
        ```bash
             vars:
                payment_type_values: [1, 2, 3, 4]
        ```
      2. as varibles whose values can be passed through the CLI when running the model
        ```jinja
            {% if var('is_test_run', default=true) %}

            limit 100

            {% endif %}
        ```
        ```bash
        dbt build --m <model.sql> --vars 'is_test_run: false'
        ```
        If we run the model and pass the value `false` then model will not use limit 100. This is super useful during development when you are working with large tables.
        You can use variables in models as well as in config files for parameter values.
        Example
        ```yaml
        tests: 
              - accepted_values:
                  values: "{{ var('payment_type_values') }}"
                  severity: warn
                  quote: false
        ```
- seed
  - you can add a csv file in your repo
  - to create a table, run dbt seed
    ```bash
    dbt seed # appends any file updates to the table 
    dbt seed --full-refresh # drops and creates a new table 
    ```
  - dbt generates a schema automatically. In order to specify a schema, you can add configs for the seed in the project.yml
    ```yaml
        seeds: 
        taxi_rides_ny:
            taxi_zone_lookup:
                +column_types: # Config indicated by + and applies to all files
                    locationid: numeric
    ```
    - *Note the seed lineage icon, it's a green seed.*
- snapshots
  - are a way to persist changes in a table and maintain a history
  - are a batch-based approach to change data capture. 
  - if you have a table where a certain data gets overwritten (in case of a OLTP database for example, an orders table where order status changes from pending to shipped), you can use snapshots to create a history of changes.
  - a snapshot is essentially a sql query that records the change using a strategy
  - there are two built-in strategies to create snapshots
    - timestamp (recommended)
    - check (when a timestamp or data_loaded field is not available.)
  - you create a new sql file and add your snapshot select query along with configs there. You can also add configs to your project.yml.
  - they can used/referenced in your downstream models using the `ref` macro
  - as a best practice, snapshot tables should be saved in a separate schema (s.t it's hard to drop them)
    ```sql
        {% snapshot orders_snapshot %}

        {{
            config(
            target_database='analytics',
            target_schema='snapshots',
            unique_key='id',

            strategy='timestamp',
            updated_at='updated_at',
            )
        }}

        select * from {{ source('jaffle_shop', 'orders') }}

        {% endsnapshot %}
    ```
    - to run the model, create a new table based on the query
        ```bash
        dbt snapshot 
        ```
  - snapshot models need to be run frequently in order to maintain the history
  - more details [here](https://docs.getdbt.com/docs/build/snapshots)
- dbt build vs run
  - ```bash
        dbt run   # runs all models
        dbt build # runs all models, seeds and tests
        dbt build --select fact_trips # runs only the fact_trips model
        dbt build --select +fact_trips # runs all dependencies in order based on the lineage
    ```

- tests
  - also a sql query that returns number of records based on a failure condition
  - defined on a column in the .yml file.
  - dbt provides built-in tests for verifying unique, not null, accepted values, a foreign key to another table.
  ```yaml
  columns:
        # tests for unique and non null values
        - name: tripid
        description: Primary key for this table, generated with a concatenation of vendorid+pickup_datetime
        tests:
            - unique:
                severity: warn
            - not_null:
                severity: warn
        # test for foreign key
        - name: dropoff_locationid 
                    description: locationid where the meter was engaged.
                    tests:
                    - relationships:
                        to: ref('taxi_zone_lookup')
                        field: locationid
                        severity: warn
        # test for accepted values
         - name: Payment_type 
            description: >
              A numeric code signifying how the passenger paid for the trip.
            tests: 
              - accepted_values:
                  values: "{{ var('payment_type_values') }}"
                  severity: warn
                  quote: false
  ```
  - you can create custom tests as sql queries and save in macros
  - you can also import tests from other projects through packages, for example `db_utils` has many useful tests.
  - Running tests
    ```bash
    dbt test -m <model.sql>
    dbt test --select model_name
    dbt test --select +model_name # runs test on specified model and it's ancestors
    dbt test --select model_name+ # runs test on specified model and it's children
    ```

- documentation
  - dbt also generates documentation based on the model and column descriptions provided in the yaml files and render it as a website hosted on dbt cloud or localhost.
  - you add schema.yml to your `core`, `macros` and `seeds` folders for documentation.

Deploying the project 
- deployment enviroment uses a separate schema and different user for the DWH
- workflow 
  - develop in a project branch
  - run tests locally and create documentation
  - merge into the main branch
  - run the main branch in production environment
  - schedule the models
- dbt jobs
  - a job is a group of dbt commands
  - it's good to split dbt build into muliptle commands to have separate logs
  - you can create and schedule a job in dbt 
- CI/CD
  - If you are developing the dbt cloud IDE, then after the first commit, you are not able to push changes directly into main
    - main becomes the production branch after the first commit.
    - after that, you can create a new branch, make your changes and submit a PR
    - You can set up CI using Github Webhooks for your project.
    - create a new environment for deployment called production under the environment tab. The existing environment is called development. 
    - You'll need a service account (ideally different from your development user account) and a dataset (or schema) for deployment credentials.
    - create a new job for the production environment. (add commands for dbt seed, dbt run, dbt test). Set a schedule. And add a webhook.
    - whenever the job runs, it clones the repo first, so that it has the latest code.
    - when a PR is ready to be merged, github will send a webhook to dbt to create a job that we defined earlier and start a run.
    - this run for the CI is done against a temprary schema and the code is merged only if it is successful.
  - If you are using a local setup, you can setup a production env with following steps
    - you can define multiple environments under `outputs` section of your project in the `profile.yml` (~\.dbt\profile.yml). `target` specifies the default environment which is typically *dev*. So dbt build would run in *dev* by default.
        ```yaml
        ny_taxi:
        target: dev 
        outputs:
            dev:
            type: bigquery
            dataset: ny_taxi
            job_execution_timeout_seconds: 300
            job_retries: 1
            keyfile: /Users/gdk/google-cloud-sdk/dtc-de-2023.json
            location: US
            method: service-account
            priority: interactive
            project: dtc-de-2023-398823
            threads: 4
            keepalives_idle:  0 #default 0, indicating the system default
            prod:
            type: bigquery
            dataset: prod   # name of your production dataset (should already exist in biquery)
            job_execution_timeout_seconds: 300
            job_retries: 1
            keyfile: /Users/gdk/google-cloud-sdk/dtc-de-2023.json #another service account
            location: US
            method: service-account
            priority: interactive
            project: dtc-de-2023-398823
            threads: 4
            keepalives_idle:  0 #default 0, indicating the system default

        ```
    - In order to run your project in production, specify the target as 
        ```bash
                dbt build -t prod
        ```
    - you can create a job in bash pr python and schedule it using cron

**Contents description**
- models > staging
  1. stg_yellow_trips.sql: select unique rows from yellow taxi data, rename and type cast columns to make it consistent across taxi services (green, fhv etc)
  2. stg_green_trips.sql: select unique rows from green taxi data, rename and type cast columns to make it consistent across taxi services (green, fhv etc)
- models > core
  1. dim_monthly_zone_reveneue.sql: calculate monthly revenue from fare, taxes, tips, surcharge etc.
  2. dim_zones.sql: selects all columns from taxi lookup (using the seed ref) cleans values for `Boro` service by replacing with `Green`. 
  3. fact_trips.sql: Joins trips from both yellow and taxi models
- models > schema.yml : `sources` config is mandatory, to resolve the sources to actual tables. You can add `models` for adding tests and documentation.
- seeds
  - taxi_zone_lookup.csv: Lookup file downloaded from ()
- analyses
  - not all sql queries need to saved as tables or views
  - some sql queries that are complex and are only meant for analysis or connecting with data viz tool are saved in this folder
  - sql file in this folder are compiled but not executed. 
  - the compiled code can be accessed from the `target` folder and used in an query editor or BI tool.
- scripts: python scripts to ingest the ny taxi data 
- profiles_bq.yml: sample  profiles config for Bigquery
- profiles_postgres.yml: sample  profiles config for Postgres


Troubleshooting: 

- Multiple profiles in ~/.dbt/profile.yml did not work for me (dbt debug gave internal error). Make sure that it matches the first key in profile.yml or you have only one key in your profile. [more](https://stackoverflow.com/questions/62742369/while-running-the-dbt-run-command-gets-error)


### Extraaas: Explore the dataset using Metabase

Setup Metabase on mac M1 locally
- Install java (if you do not have already)
    ```bash 
    brew install openjdk 
    ```
- Add JAVA to your PATH
    ```bash 
    echo 'export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"' >> ~/.zshrc
    ```
-  Use the source command to apply the changes to the current session.
    ```bash  
    source ~/.zshrc
    ```
- Check java version
  ```bash 
  java -version
  ```
- Download Metabase jar from [link](https://www.metabase.com/start/oss/jar).
- Create a new directory for metabse and move the jar file in that folder. 
- Go to the folder and run
    ```bash 
    java -jar metabase.jar
    ```
- This will install metabase and start the server on `localhost:3000`
- In case your port is already being used, it will throw an error. Free the port and run the command again.
- Follow the instructions to create a profile, connect with your data source and start dashboarding!