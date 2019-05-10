(1) "npm install" in console should install all dependencies from package.json
Otherwise, they can be viewed in said file

(2) "npm cleaner.js" will run the cleaning script and output to both "clean_sample.csv" and "cleanSQL.db"

The script assumes that the input file is named "sample_organizations.csv" and that it is located in the local directory. This can be modified to take file-name/location from the command-line or from another directory if needed.

(3) "npm query.js" will run the assortment of SQL queries on the database, provided you ran "cleaner.js" first to create one.

Currently, the output of these queries is printed to console. I am attempting to print them to a csv output but am currently wrestling with Node and its asynchronous SQL queries (and everything else)
