const csv = require('csv');
const fs = require('fs');
const validUrl = require('valid-url')
const request = require('request')
const validator = require("email-validator");
const validate = require('uuid-validate');
const sqlite3 = require('sqlite3').verbose();

// headers added as per request
var newHeaders = {
  bad_company_name: "",
  secondary_role: "",
  tertiary_role: "",
  cleaned_domain: "",
  bad_url: "",
  does_domain_match_cleaned_domain: "",
  created_cb_url: "",
  cleaned_email: "",
  bad_email: "",
  facebook_id: "",
  linkedin_id: "",
  twitter_id: "",
  is_uuid_valid: "",
  number_of_names: ""
};

// order the columns by header for stringifying
var finalHeaders = [
  "company_name",
  "bad_company_name",
  "roles",
  "secondary_role",
  "tertiary_role",
  "homepage_url",
  "bad_url",
  "domain",
  "cleaned_domain",
  "does_domain_match_cleaned_domain",
  "country_code",
  "state_code",
  "region",
  "city",
  "address",
  "status",
  "short_description",
  "category_list",
  "category_group_list",
  "funding_rounds",
  "funding_total_usd",
  "founded_on",
  "last_funding_on",
  "closed_on",
  "employee_count",
  "email",
  "cleaned_email",
  "bad_email",
  "phone",
  "facebook_url",
  "facebook_id",
  "linkedin_url",
  "linkedin_id",
  "permalink",
  "cb_url",
  "created_cb_url",
  "logo_url",
  "twitter_url",
  "twitter_id",
  "aliases",
  "number_of_names",
  "uuid",
  "is_uuid_valid",
  "created_at",
  "updated_at",
  "primary_role",
  "type"
]

var input = fs.createReadStream("sample_organizations.csv");
// var output = fs.createWriteStream("clean_sample.csv", {encoding: 'utf-8' });
var db = new sqlite3.Database('cleanSQL.db');


//var inputcsv = process.argv[2]; //input file on command line

// parses into objects with columns: TRUE header, will be arrays/no header row if FALSE
// "record" in all of the below transform functions is an object comprised of one row of company info
var parsed = csv
  .parse({
    columns : true
  });

// add new headers to each record object
var add_columns = csv
  .transform(record => {
    return Object.assign(record, newHeaders);
  })

// deletes names corresponding to those 2 options only
// marks TRUE in bad_company_name column
var company_name_cleaner = csv
  .transform(record => {
    if (record.company_name == "XXXXX" || record.company_name == "..."){
      record.company_name = "";
      record.bad_company_name = true;
      return record;
    } else {
      return record;
    }
  })

// separates multiple roles (if applicable) into distinct role columns
var roles_cleaner = csv
  .transform(record => {
    let rolesList = record.roles;
    let rolesArray = rolesList.split(",");
    if(rolesArray[2]) {
      record.tertiary_role = rolesArray[2];
      record.secondary_role = rolesArray[1];
      record.roles = rolesArray[0];
      return record;
    } else if (rolesArray[1]) {
      record.secondary_role = rolesArray[1];
      record.roles = rolesArray[0];
      return record;
    } else {
      record.roles = rolesArray[0];
      return record;
    }
  })

// uses validUrl package for validation
// if homepage_url fails validUrl, puts TRUE in bad_url column
// bad_url will include records with no homepage_url
// puts "cleaned" urls in cleaned_domain column, ignoring bit.ly addresses
var homepage_url_cleaner = csv
  .transform(record => {
    if (!validUrl.isUri(record.homepage_url)) {
      record.bad_url = true;
    }
    if (record.homepage_url && record.homepage_url.toLowerCase().indexOf("bit.ly") === -1) {
      record.cleaned_domain = record.homepage_url.replace(/^(http(s?):\/\/)?(www(1?)\.)?/i, "").split('/')[0]
      return record;
    }
    return record;
  })

// compares domain with cleaned_domain
//
var domain_checker = csv
  .transform(record => {
    if (record.domain == record.cleaned_domain) {
      record.does_domain_match_cleaned_domain = true;
    } else {
      record.does_domain_match_cleaned_domain = false;
    }
    return record;
  })

// checks input url for response using HEAD request
var urlChecker = function(input_url) {
  request({method: "HEAD", uri: input_url}, function(error, response, body) {
    console.log(response && response.statusCode)
    // return ((!error && response.statusCode == 200) ? true : false);
  })
}

// assumes permalink is provided
// if cb_url is not provided, will created_cb_url using id in permalink
// uses urlChecker to verify url is live; however, crunchbase seems to have captcha protection(?) blocking check
var cb_checker = csv
  .transform(record => {
    if (record.permalink && !record.cb_url) {
      let permalink_id = record.permalink.replace(/^\/organization\//,"")
      record.created_cb_url = "https://www.crunchbase.com/organization/" + permalink_id
    }
    // urlChecker(record.cb_url);
    return record;
  })

// assumes a-z, A-Z, 0-9, period, underscore, and hyphen are the only characters allowed in email names
// also removes trailing periods, which seems to be the main cause of bad_email
// uses email_validator to validate email, sets bad_email to TRUE if invalid
var email_cleaner = csv
  .transform(record => {
    if (record.email) {
      record.cleaned_email = record.email.match(/[a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+/)[0].replace(/\.$/, "");
      if (!validator.validate(record.cleaned_email)) {
        record.bad_email = true;
      }
    }
    return record;
  })

// attempts to extract facebook_id from facebook_url
// huge variety of cases (multiple or missing http/s/www, page/s, .com, trailing characters, non-facebook urls)
// of edge cases, only covered "facebook.com/google" because most common
// other edge cases (misspellings/typos, incomplete urls, etc) must be included manually case-by-case
var facebook_id_extractor = csv
  .transform(record => {
    if (record.facebook_url && record.facebook_url.toLowerCase().includes("facebook")) {
      let id = record.facebook_url
        .replace(/^(.*?)(facebook)(\.com)?(\/)?(page)?(s?)(\/?)/i, "")
        .replace(/\/.*/, "") // assumes first slash and everything after at this point is garbage
        .replace(/\?.*/, "") // assumes any question mark and everything after at this point is garbage
        .toLowerCase();
      if (!id.includes("google")) {record.facebook_id = id };
    }
    return record;
  })

// similar issues as with facebook_id, but to a lesser degree
// filters for [ company, company-beta, in, edu, school, pub, groups ] in the url, have not found any others
var linkedin_id_extractor = csv
  .transform(record => {
    if (record.linkedin_url && record.linkedin_url.toLowerCase().includes("linkedin")) {
      let id = record.linkedin_url
        .replace(/^(.*?)(linkedin)(\.com)?(\/)?(company(\-beta)?)?(in)?(edu)?(school)?(pub)?(groups)?(\/?)/i, "")
        .replace(/\/.*/, "") // assumes first slash and everything after at this point is garbage
        .replace(/\?.*/, "") // assumes any question mark and everything after at this point is garbage
        .toLowerCase();
      if (!id.includes("google")) {record.linkedin_id = id };
    }
    return record;
  })

// similar issues as facebook_id_extractor, but to an even lesser degree
// haven't seen inputs in an @twitterHandler format yet, so current code will only accept below formats; can change as necessary
var twitter_id_extractor = csv
  .transform(record => {
    if (record.twitter_url && record.twitter_url.toLowerCase().includes("twitter")) {
      let id = record.twitter_url
        .replace(/^(.*?)(twitter)(\.com)?(\/)?/i, "")
        .replace(/\/.*/, "") // assumes first slash and everything after at this point is garbage
        .replace(/\?.*/, "") // assumes any question mark and everything after at this point is garbage
        .toLowerCase();
      if (!id.includes("google")) {record.twitter_id = id };
    } else if (record.twitter_url && record.twitter_url[0] == "@") {
      record.twitter_id = record.twitter_url.substring(1).toLowerCase();
    }
    return record;
  })

// uses the uuid-validate package to validate
var uuid_validator = csv
  .transform(record => {
    if (record.uuid) {
      validate(record.uuid) ? record.is_uuid_valid = true : record.uuid = false;
    }
    return record;
  })

// number_of_names counts names plus aliases
var alias_checker = csv
  .transform(record => {
    if (!record.aliases) {
      record.number_of_names = 1;
    } else {
      record.number_of_names = record.aliases.split(",").length + 1;
    }
    return record;
  })

// chose sqlite because lightweight, good for reading/writing to disk
// create table if not exists to cover base case
// just going to leave db connection open (not sure where to close without constantly re-opening/closing, stackoverflow says it's ok)
// ISSUE FIXED: had to chain create and insert commands to deal with async issue
////// "no such table" bug when running script for first time, async issue with table creation?
////// runs fine after table is already created (just run again a second time)

var sql_insertion = csv
  .transform(record => {

    let createTableQuery = `CREATE TABLE IF NOT EXISTS cleanSql(${finalHeaders.join(", ")})`;

    let recordAsOrderedArray = finalHeaders.map(key => record[`${key}`]); //array of single-quoted strings in correct order
    let recordAsSqlString = recordAsOrderedArray
      .map(entry => entry.toString().replace(/\'/g, "''"))
      .map(entry => `'${entry}'`)
      .join(", "); // single quotes and join unnecessary?
    let insertRecordQuery = `INSERT INTO cleanSql VALUES(${recordAsSqlString})`

    db.run(createTableQuery, () => {db.run(insertRecordQuery)});

    return record;
  })

var stringifier = csv
  .stringify({
    header: true,
    columns: finalHeaders
  })

input
  .pipe(parsed)
  .pipe(add_columns)
  .pipe(company_name_cleaner)
  .pipe(roles_cleaner)
  .pipe(homepage_url_cleaner)
  .pipe(domain_checker)
  .pipe(cb_checker)
  .pipe(email_cleaner)
  .pipe(facebook_id_extractor)
  .pipe(linkedin_id_extractor)
  .pipe(twitter_id_extractor)
  .pipe(uuid_validator)
  .pipe(alias_checker)
  .pipe(sql_insertion)
  .pipe(stringifier)
  .pipe(fs.createWriteStream("clean_sample.csv"))
