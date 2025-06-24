-- Create the role if not exists
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'aoe2user') THEN
      CREATE ROLE aoe2user WITH LOGIN PASSWORD 'secretpassword';
   END IF;
END
$$;

-- Create the aoe2user database for pgAdmin compatibility
CREATE DATABASE aoe2user OWNER aoe2user;

-- Grant privileges
\connect aoe2db
GRANT ALL PRIVILEGES ON DATABASE aoe2db TO aoe2user;

\connect postgres
GRANT ALL PRIVILEGES ON DATABASE aoe2user TO aoe2user;
