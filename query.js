const csv = require('csv');
const fs = require('fs');
const sqlite3 = require('sqlite3').verbose();

var db = new sqlite3.Database('cleanSQL.db');
var finalObject = {};

// sql query function
var queryer = function(sql_query, var_result) {
  db.all(sql_query, (err, results) => {
    var_result = Object.values(results[0])[0];
    console.log(sql_query + " is " + var_result);
  })
}

// number of companies with exactly 1 company name
var sql_one_name = `SELECT SUM(CASE WHEN CAST(number_of_names AS NUMERIC) = 1 THEN 1 ELSE 0 END) FROM cleanSql`;
var one_name;
queryer(sql_one_name, one_name);

// number of companies with 2 company names
var sql_two_names = `SELECT SUM(CASE WHEN CAST(number_of_names AS NUMERIC) = 2 THEN 1 ELSE 0 END) FROM cleanSql`;
var two_names;
queryer(sql_two_names, two_names);

// number of companies with more than 2 company names
var sql_more_than_two_names = `SELECT SUM(CASE WHEN CAST(number_of_names AS NUMERIC) > 2 THEN 1 ELSE 0 END) FROM cleanSql`;
var more_than_two_names;
queryer(sql_more_than_two_names, more_than_two_names);

// number of companies with missing homepage_url or domain data
// all records missing either are missing both in this sample
var sql_missing_homepage_or_domain = `SELECT SUM(CASE WHEN homepage_url = "" OR domain = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var missing_homepage_or_domain;
queryer(sql_missing_homepage_or_domain, missing_homepage_or_domain);

// number of companies with bad URL data
var sql_bad_url = `SELECT SUM(CASE WHEN bad_url = "true" THEN 1 ELSE 0 END) FROM cleanSql`;
var bad_url;
queryer(sql_bad_url, bad_url);

// number of companies where domain DOES NOT match the "cleaned_domain" column.
var sql_domain_match = `SELECT SUM(CASE WHEN does_domain_match_cleaned_domain = "false" THEN 1 ELSE 0 END) FROM cleanSql`;
var domain_match;
queryer(sql_domain_match, domain_match);

// number of companies with missing Facebook URLs
var sql_missing_fb_urls = `SELECT SUM(CASE WHEN facebook_url = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var missing_fb_urls;
queryer(sql_missing_fb_urls, missing_fb_urls);

// number of companies with Facebook URLs but no Facebook IDs
var sql_fb_url_no_id = `SELECT SUM(CASE WHEN facebook_url <> '' AND facebook_id = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var fb_url_no_id;
queryer(sql_fb_url_no_id, fb_url_no_id);

// number of companies with missing LinkedIn URLs
var sql_no_li_url = `SELECT SUM(CASE WHEN linkedin_url = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var no_li_url;
queryer(sql_no_li_url, no_li_url);

// number of companies with Linkedin URLs but no LinkedIn IDs
var sql_li_url_no_id = `SELECT SUM(CASE WHEN linkedin_url <> '' AND linkedin_id = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var li_url_no_id;
queryer(sql_li_url_no_id, li_url_no_id);

// number of companies with missing Twitter URLs
var sql_no_twit_url = `SELECT SUM(CASE WHEN twitter_url = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var no_twit_url;
queryer(sql_no_twit_url, no_twit_url);

// number of companies with Twitter URLs but no Twitter IDs
var sql_twit_url_no_id = `SELECT SUM(CASE WHEN twitter_url <> '' AND twitter_id = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var twit_url_no_id;
queryer(sql_twit_url_no_id, twit_url_no_id);

// number of companies that have domain, Facebook, LinkedIn and Twitter IDs
var sql_domain_fb_li_twit = `SELECT SUM(CASE WHEN domain <> '' AND facebook_id <> '' AND linkedin_id <> '' AND twitter_id <> '' THEN 1 ELSE 0 END) FROM cleanSql`;
var domain_fb_li_twit;
queryer(sql_domain_fb_li_twit, domain_fb_li_twit);

// number of companies that have domain and Facebook IDs but no LinkedIn or Twitter
var sql_domain_fb_no_li_no_twit = `SELECT SUM(CASE WHEN domain <> '' AND facebook_id <> '' AND linkedin_id = "" AND twitter_id = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var domain_fb_no_li_no_twit;
queryer(sql_domain_fb_no_li_no_twit, domain_fb_no_li_no_twit);

// number of companies that have domain and Linkedin IDs but no Facebook or Twitter
var sql_domain_li_no_fb_no_twit = `SELECT SUM(CASE WHEN domain <> '' AND linkedin_id <> '' AND facebook_id = "" AND twitter_id = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var domain_li_no_fb_no_twit;
queryer(sql_domain_li_no_fb_no_twit, domain_li_no_fb_no_twit);

// number of companies that have domain and Twitter IDs but no LinkedIn or Facebook
var sql_domain_twit_no_li_no_fb = `SELECT SUM(CASE WHEN domain <> '' AND twitter_id <> '' AND linkedin_id = "" AND facebook_id = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var domain_twit_no_li_no_fb;
queryer(sql_domain_twit_no_li_no_fb, domain_twit_no_li_no_fb);

// number of companies that have domain, Facebook and Twitter IDs but no LinkedIn
var sql_domain_fb_twit_no_li = `SELECT SUM(CASE WHEN domain <> '' AND facebook_id <> '' AND twitter_id <> '' AND linkedin_id = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var domain_fb_twit_no_li;
queryer(sql_domain_fb_twit_no_li, domain_fb_twit_no_li);

// number of companies that have domain, Facebook and Linkedin IDs but no Twitter
var sql_domain_fb_li_no_twit = `SELECT SUM(CASE WHEN domain <> '' AND facebook_id <> '' AND linkedin_id <> '' AND twitter_id = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var domain_fb_li_no_twit;
queryer(sql_domain_fb_li_no_twit, domain_fb_li_no_twit);

// number of companies that have domain, Twitter and Linkedin IDs but no Facebook
var sql_domain_twit_li_no_fb = `SELECT SUM(CASE WHEN domain <> '' AND twitter_id <> '' AND linkedin_id <> '' AND facebook_id = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var domain_twit_li_no_fb;
queryer(sql_domain_twit_li_no_fb, domain_twit_li_no_fb);

// number of companies that have domain but no Facebook, LinkedIn and Twitter IDs
var sql_domain_no_fb_no_li_no_twit = `SELECT SUM(CASE WHEN domain <> '' AND facebook_id = "" AND linkedin_id = "" AND twitter_id = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var domain_no_fb_no_li_no_twit;
queryer(sql_domain_no_fb_no_li_no_twit, domain_no_fb_no_li_no_twit);

// number of companies with invalid UUIDs
// only counts records with UUIDs
var sql_invalid_uuid = `SELECT SUM(CASE WHEN is_uuid_valid = "false" THEN 1 ELSE 0 END) FROM cleanSql`;
var invalid_uuid;
queryer(sql_invalid_uuid, invalid_uuid);

// number of companies with missing cb_url data
var sql_no_cb_url = `SELECT SUM(CASE WHEN cb_url = "" THEN 1 ELSE 0 END) FROM cleanSql`;
var no_cb_url;
queryer(sql_no_cb_url, no_cb_url);

// number of companies with created_cb_url values
var sql_created_cb_url = `SELECT SUM(CASE WHEN created_cb_url <> '' THEN 1 ELSE 0 END) FROM cleanSql`;
var created_cb_url;
queryer(sql_created_cb_url, created_cb_url);

// number of companies with duplicate domains, duplicate Facebook/Linkedin/Twitter IDs
// can't figure out where to check for duplicate domains
// check for ids includes all companies where at least 2 of the 3 match
var sql_duplicate_ids = `SELECT SUM(CASE WHEN facebook_id = linkedin_id OR facebook_id = twitter_id OR linkedin_id = twitter_id THEN 1 ELSE 0 END) FROM cleanSql`;
var duplicate_ids;
queryer(sql_duplicate_ids, duplicate_ids);

csv
  .stringify([{ a: '1', b: '2' }], { header: false })
  .pipe(fs.createWriteStream("sql_results.csv"))
