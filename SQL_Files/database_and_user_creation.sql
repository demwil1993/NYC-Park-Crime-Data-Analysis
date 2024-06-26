CREATE DATABASE nyc_crime;

CREATE USER crime_user WITH ENCRYPTED PASSWORD '*******';

GRANT ALL ON DATABASE nyc_crime TO crime_user;
