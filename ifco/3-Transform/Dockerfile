# Stage 1: PostgreSQL Setup
FROM postgres:latest AS postgres

# Set environment variables for PostgreSQL
ENV POSTGRES_USER=ccardoso
ENV POSTGRES_PASSWORD=ccardoso
ENV POSTGRES_DB=bronze

# Stage 2: Python & dbt Setup
FROM python:3.10-slim-buster AS dbt

# Update and install necessary system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set up working directory for dbt
WORKDIR /usr/src/dbt/dbt_project

# Upgrade pip and install dbt and dependencies
RUN pip install --upgrade pip
RUN pip install dbt-postgres==1.3.1

# Copy over profiles directory and any configuration files for dbt (like profiles.yml)
COPY ifco_business_intelligence/profiles/profiles.yml /usr/src/dbt/dbt_project/profiles

# Default command to keep the container running and dbt service alive
CMD ["sh", "-c", "dbt deps && dbt build --profiles-dir profiles && sleep infinity"]

# Stage 3: Final Image Assembly
# We use a base image with PostgreSQL, and copy over dbt and its dependencies
FROM postgres:latest

# Set environment variables for PostgreSQL (again, to persist in final image)
ENV POSTGRES_USER=ccardoso
ENV POSTGRES_PASSWORD=ccardoso
ENV POSTGRES_DB=bronze

# Copy the initialized database files from the first stage
COPY --from=postgres /docker-entrypoint-initdb.d /docker-entrypoint-initdb.d

# Copy dbt and Python setup from the dbt stage
COPY --from=dbt /usr/local/lib/python3.10 /usr/local/lib/python3.10
COPY --from=dbt /usr/local/bin/dbt /usr/local/bin/dbt
COPY --from=dbt /usr/src/dbt/dbt_project /usr/src/dbt/dbt_project

# Expose the PostgreSQL port
EXPOSE 5432
