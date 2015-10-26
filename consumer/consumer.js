var config = require('./credentials.js');
var Readable = require('kinesis-readable')(config);
var Redis = require('ioredis');
var R = require('ramda');

// Initialize redis
var redis = new Redis();

// Kinesis options
var readable = new Readable({
  latest: false
});

readable
.on('data', function (records) {
  processRecord(records[0].Data.toString());
})
.on('checkpoint', function (sequenceNumber) {
  // TODO Add sequenceNumber to redis
  console.log(sequenceNumber);
})
.on('error', function (error) {
  console.error(error);
});

function processRecord (record) {
  var obj = JSON.parse(record);
  var pipeline = redis.pipeline();
  var user = obj.metadata.user;
  var elements = obj.elements;

  // Only process ways
  var ways = R.filter(R.propEq('type', 'way'), elements);

  ways.forEach(function (way) {
    var tags = R.keys(way.tags);
    // Process buildings
    if (R.contains('building', tags)) {
      pipeline.zincrby('ogp:buildings', 1, user);
    }

    // Process highways
    if (R.contains('highway', tags)) {
      pipeline.zincrby('ogp:highways', 1, user);
    }

    // Add way to timeline of each user
    pipeline.lpush('ogp:timeline:' + user, JSON.stringify(way));
    pipeline.ltrim('ogp:timeline:' + user, 1000);

    // Add way to global timeline
    pipeline.lpush('ogp:timeline', JSON.stringify(way));
    pipeline.ltrim('ogp:timeline', 1000);
  });

  // Add num_changes to global count
  pipeline.zincrby('ogp:changes', obj.metadata.num_changes, user);

  // Execute pipeline
  pipeline.exec(function (err, results) {
    if (err) console.error(err);
  });
}
