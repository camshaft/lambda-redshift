/**
 * Module dependencies
 */

var AWS = require('aws-sdk');
var pg = require('pg');
var Path = require('path');
var secrets = require('./secrets');
var credentials = 'aws_access_key_id=' + secrets.AWS_ACCESS_KEY_ID +
                  ';aws_secret_access_key=' + secrets.AWS_SECRET_ACCESS_KEY;

var post = " gzip ACCEPTINVCHARS ' ' TRUNCATECOLUMNS TRIMBLANKS TIMEFORMAT 'epochmillisecs';";
var jsonpathsBucket = secrets.JSONPATHS_BUCKET;

exports.handler = function(event, context) {
  var bucket = event.Records[0].s3.bucket.name;
  var key = event.Records[0].s3.object.key;
  var withoutRoot = schemaName(key);

  var q = '' +
    " COPY atomic." + normalize(withoutRoot) +
    " FROM 's3://" + bucket + '/' + key + "'" +
    " CREDENTIALS '" + credentials + "'" +
    " JSON " + jsonschema(withoutRoot) +
    post;

  pg.connect(secrets.REDSHIFT_URL, function(err, client, done) {
    if (err) return context.fail(err);

    client.query(q, function(err, result) {
      done();
      if (err) return context.fail(err);
      return context.done();
    });
  });
};

function jsonschema(key) {
  if (key === 'events') return "'auto'";
  return "'s3://" + jsonpathsBucket + '/' + key + ".json'";
}

function schemaName(key) {
  return Path.dirname(key).replace(/^enriched\//, '');
}

function normalize(key) {
  return key.replace(/[\.\-\/]/g, '_');
}
