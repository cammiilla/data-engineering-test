# Use the official PostgreSQL image from Docker Hub
FROM postgres:latest

# Set environment variables for PostgreSQL
ENV POSTGRES_USER=ccardoso
ENV POSTGRES_PASSWORD=ccardoso
ENV POSTGRES_DB=develop

# Add custom SQL file to initialize the database
COPY init.sql /docker-entrypoint-initdb.d/

# Expose the port PostgreSQL listens on
EXPOSE 5432