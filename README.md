
# IFCO ELT Pipeline Project

## Project Overview

This project is an ELT (Extract, Load, Transform) pipeline that processes raw data into a PostgreSQL database, with transformations handled by dbt. The pipeline is structured into three main stages, each located in its own folder:

1. **Extract** - Processes raw data and outputs it in parquet format.
2. **Load** - Loads the parquet data into a PostgreSQL database.
3. **Transform** - Transforms the data using dbt in Docker.

This README will guide you through setting up dependencies with Poetry, running each stage, and querying the transformed data.

---

## Folder Structure

The project is organized as follows:

```
- 1-Extract/
  - raw_data/              # Folder containing raw data files
  - data_extraction.py      # Python script to process raw data and export it to parquet format

- 2-Load/
  - DataBase/
    - Dockerfile           # Dockerfile for PostgreSQL setup
    - docker-compose.yml   # Docker Compose file for PostgreSQL database setup
  - Data/                  # Folder where parquet files are stored after extraction
  - load_data.py           # Python script to load parquet data into PostgreSQL database
  - DataBaseCredentials/   # Folder containing credentials for the PostgreSQL database

- 3-Transform/
  - Dockerfile             # Dockerfile to build a dbt image
  - docker-compose.yml     # Docker Compose file to run dbt transformations
  - dbt_project/           # dbt project with SQL transformation models
    - models/
      - marts/
        - business_intelligence/ # Final models for business intelligence transformations
          - mrt_business_intelligence_sch_test_1.sql
          - mrt_business_intelligence_sch_test_2.sql
          - mrt_business_intelligence_sch_test_3.sql
          - mrt_business_intelligence_sch_test_4.sql
          - mrt_business_intelligence_sch_test_5.sql

- pyproject.toml            # Poetry configuration file for Python dependencies
```

---

## Prerequisites

1. **Docker** - Required for PostgreSQL and dbt setup.
2. **Python** - Required to run the extraction and load scripts.
3. **Poetry** - Used for managing Python dependencies.
4. **dbt** - Transformation is handled within Docker using dbt, which will be set up in the Transform stage.

---

## Step 1: Set Up Python Dependencies with Poetry

1. **Install Poetry** (if not already installed). Follow instructions at [Poetry's official website](https://python-poetry.org/docs/#installation).

2. **Install dependencies**:
   In the root folder (where the `pyproject.toml` file is located), run:
   ```bash
   poetry install
   ```

3. **Activate the Poetry environment**:
   ```bash
   poetry shell
   ```

   This activates the virtual environment managed by Poetry, allowing you to run the Python scripts with all necessary dependencies.

---

## Step 2: Extract Raw Data

Run the `data_extraction.py` script from the root directory to process the raw data and save it in the `Data/` folder as parquet files:

```bash
python 1-Extract/data_extraction.py
```

This script reads data from `1-Extract/raw_data/`, processes it, and writes the output to the `2-Load/Data/` folder in parquet format.

---

## Step 3: Set Up and Load PostgreSQL Database

1. **Build the PostgreSQL Docker image** by running the following command in the root folder:

   ```bash
   docker build -t database 2-Load/DataBase/
   ```

2. **Start the database container** using Docker Compose:

   ```bash
   docker-compose -f 2-Load/DataBase/docker-compose.yml up -d
   ```

   This command will set up a PostgreSQL database instance, ready for data loading. Confirm the database is running by checking Docker containers or connecting to the container if needed.

3. Run the `load_data.py` script to load the parquet files into the PostgreSQL database. Be sure to include the path to the credentials file:

   ```bash
   python 2-Load/load_data.py --credentials_path 2-Load/DataBaseCredentials/credentials.json
   ```

   This script reads the parquet files from the `2-Load/Data/` folder, uses the credentials from the provided JSON file, and loads the data into appropriate tables in the PostgreSQL database.

---

## Step 4: Transform Data Using dbt

1. **Build the dbt Docker image**:

   ```bash
   docker build -t dbt 3-Transform/
   ```

2. **Start the dbt container** to run the transformations:

   ```bash
   docker-compose -f 3-Transform/docker-compose.yml up -d
   ```

   This command starts a container that runs the dbt transformations, applying all models in the `3-Transform/dbt_project/` folder to the data loaded in PostgreSQL.

---

## Accessing the PostgreSQL Database in Docker

To interact with the PostgreSQL database running in the Docker container, follow these steps:

### Step 1: List Running Docker Containers

Before accessing the PostgreSQL database, you need to find the **container ID** of the running database container. You can do this by listing all running Docker containers.

Run the following command:

```bash
docker ps
```

This will show a list of all active Docker containers, with information such as their container ID, name, and what ports they are exposing. You should see something like this:

```
CONTAINER ID   IMAGE        COMMAND                  CREATED        STATUS        PORTS                    NAMES
bf4ed5f23684   database     "docker-entrypoint.s…"   2 hours ago    Up 2 hours    0.0.0.0:5432->5432/tcp   database_container_name
```

In the output above, the **CONTAINER ID** for the PostgreSQL database container is `bf4ed5f23684`. This is the ID you'll use to access the database from within the container.

### Step 2: Access the PostgreSQL Database

Now that you have the container ID, you can access the PostgreSQL database by running the following command:

```bash
docker exec -it bf4ed5f23684 psql -U ccardoso -d develop
```

Here’s a breakdown of the command:
- `docker exec -it` is used to execute a command inside a running container.
- `bf4ed5f23684` is the container ID you retrieved from `docker ps`.
- `psql` is the PostgreSQL command-line tool that allows you to interact with the database.
- `-U ccardoso` specifies the user (`ccardoso`) to connect as.
- `-d develop` specifies the database (`develop`) to connect to.

Once you run this command, you'll be connected to the PostgreSQL database inside the container, and you can run SQL queries directly in the terminal.

---

### Step 3: Run SQL Queries

After connecting to the database, you can start running SQL queries. For example, to view all tables in the `develop` database, you can run:

```sql
\dt
```

This will display a list of all tables in the connected database.

---

## Accessing the dbt Docker Container

To access the dbt Docker container and run dbt commands directly, follow these steps:

1. **List running Docker containers** to find the container ID for the dbt image:
   ```bash
   docker ps
   ```

2. **Access the container** by running the `docker exec` command with the container ID (replace `64d97945460e` with the actual container ID from the `docker ps` output):
   ```bash
   docker exec -it 64d97945460e /bin/bash
   ```

3. Once inside the container, you can run the following useful `dbt` commands.

---

## Final Models and Solutions

The final models, which contain the solutions for each of the tests, are located in the following directory:

```
3-Transform/dbt_project/models/marts/business_intelligence/
```

The models are:

- `mrt_business_intelligence_sch_test_1.sql`
- `mrt_business_intelligence_sch_test_2.sql`
- `mrt_business_intelligence_sch_test_3.sql`
- `mrt_business_intelligence_sch_test_4.sql`
- `mrt_business_intelligence_sch_test_5.sql`

These models are applied in the **`develop`** database, under the **`gold`** schema. The table names in the database are identical to the model names (e.g., the table for `mrt_business_intelligence_sch_test_1` is named `mrt_business_intelligence_sch_test_1`).

---

## Useful dbt Commands

Once inside the dbt container, you can use various `dbt` commands to build, test, and query models:

### 1. Build the dbt models

To build the dbt models, use the `dbt build` command. This will run the models and tests:

```bash
dbt build --profiles-dir=profiles
```

### 2. Test the dbt models

To run tests on the dbt models, use the `dbt test` command:

```bash
dbt test --profiles-dir=profiles
```

### 3. View the Results of Specific Models

To view the results of specific models, use the `dbt

 show` command. Here are a few examples:

```bash
dbt show --select mrt_business_intelligence_sch_test_1 --limit 100 --profiles-dir=profiles
dbt show --select mrt_business_intelligence_sch_test_2 --limit 100 --profiles-dir=profiles
dbt show --select mrt_business_intelligence_sch_test_3 --limit 100 --profiles-dir=profiles
dbt show --select mrt_business_intelligence_sch_test_4 --limit 100 --profiles-dir=profiles
dbt show --select mrt_business_intelligence_sch_test_5 --limit 100 --profiles-dir=profiles
```

These commands will return the first 100 rows from each of the respective models.

---

## Accessing Tables Directly from PostgreSQL Database

If you prefer, you can also query the tables directly from the database without using dbt. Here’s how:

1. **Find the container ID** for the running PostgreSQL database container:
   ```bash
   docker ps
   ```

2. **Access the PostgreSQL database** inside the container:
   ```bash
   docker exec -it <container_id> psql -U ccardoso -d develop
   ```

3. **Run SQL queries** to view the tables. For example, to view the results of a specific model:
   ```sql
   SELECT * FROM gold.mrt_business_intelligence_sch_test_1;
   ```

4. **Exit the PostgreSQL terminal** by typing:
   ```bash
   \q
   ```

