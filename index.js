/**
 * Module dependencies
 */

var AWS = require('aws-sdk');
var pg = require('pg');
var secrets = require('./secrets');
var credentials = 'aws_access_key_id=' + secrets.AWS_ACCESS_KEY_ID +
                  ';aws_secret_access_key=' + secrets.AWS_SECRET_ACCESS_KEY;

var pre = "COPY " + secrets.REDSHIFT_TABLE + " FROM ";
var post = " CREDENTIALS '" + credentials + "' json 'auto' gzip ACCEPTINVCHARS ' ' TRUNCATECOLUMNS TRIMBLANKS TIMEFORMAT 'epochmillisecs';";

exports.handler = function(event, context) {
  var bucket = event.Records[0].s3.bucket.name;
  var key = event.Records[0].s3.object.key;
  // why won't you let me interpolate pg?
  var q = pre + "'s3://" + bucket + '/' + key + "'" + post;

  pg.connect(secrets.REDSHIFT_URL, function(err, client, done) {
    if (err) return context.fail(err);

    client.query(q, function(err, result) {
      done();
      if (err) return context.fail(err);
      return context.done();
    });
  });
};
