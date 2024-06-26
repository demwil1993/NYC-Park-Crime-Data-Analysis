CREATE DATABASE nyc_crime;

CREATE USER crime_user WITH ENCRYPTED PASSWORD 'Axwell34';

GRANT ALL ON DATABASE nyc_crime TO crime_user;