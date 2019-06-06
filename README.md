# TECHNOLOGY
* NodeJS
* Streams
* csv-parse library
* SQLite

# DESCRIPTION
This project uses NodeJS and relevant libraries to parse a CSV file containing thousands of company names and associated info. Node Streams are also used to efficiently process portions of the data at a time, so as to not require reading the entire 10,000 entry file into memory. This information is then cleaned of errors and bad data, such that the final clean CSV (and additional output to a SQLite database) can be used for efficient data analysis (in this case, by VC firms).

Examples of data cleaning:
- tracking incorrect company names
- cleaning company URLs/domains
- creating crunchbase links using provided ID's, if any
- tracking email information that does not adhere to RFC 5322
- extracting social media ID info

The script assumes that the input file is named "sample_orgs.csv" and that it is located in the local directory. This can be modified to take file-name/location from the command-line or from another directory if needed.

The SQLite database can be used to run SQL queries on the cleaned data. The queries provided in query.js are simply examples.

Currently, the output of these queries is printed to console.

#INSTRUCTIONS FOR USE
(1) "npm install" in console should install all dependencies from package.json
Otherwise, they can be viewed in said file

(2) "npm cleaner.js" will run the cleaning script and output to both "clean_sample.csv" and "cleanSQL.db"

(3) "npm query.js" will run the assortment of SQL queries on the database, provided you ran "cleaner.js" first to create one.
