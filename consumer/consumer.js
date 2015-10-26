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

  var geojsonDiff = {
    'type': 'FeatureCollection',
    'features': [],
    'properties': obj.metadata
  };

  ways.forEach(function (way) {
    var tags = R.keys(way.tags);
    // Process buildings
    if (R.contains('building', tags)) {
      pipeline.zincrby('ogp:buildings', 1, user);
      pipeline.zincrby('ogp:buildings', 1, 'total');
    }

    // Process highways
    if (R.contains('highway', tags)) {
      pipeline.zincrby('ogp:highways', 1, user);
      pipeline.zincrby('ogp:highways', 1, 'total');
    }

    // Process waterways
    if (R.contains('waterway', tags)) {
      pipeline.zincrby('ogp:waterways', 1, user);
      pipeline.zincrby('ogp:waterways', 1, 'total');
    }
    // Add way to timeline of each user
    pipeline.lpush('ogp:timeline:' + user, JSON.stringify(way));
    pipeline.ltrim('ogp:timeline:' + user, 1000);

    // Process to geojson
    var geojsonWay = toGeojson(way);
    geojsonDiff.features.push(geojsonWay);
  });

  // Add changeset to global timeline
  pipeline.lpush('ogp:timeline', JSON.stringify(geojsonDiff));
  pipeline.ltrim('ogp:timeline', 100);

  // Add num_changes to global count
  pipeline.zincrby('ogp:changes', obj.metadata.num_changes, user);
  pipeline.zincrby('ogp:changes', obj.metadata.num_changes, 'total');

  // Execute pipeline
  pipeline.exec(function (err, results) {
    if (err) console.error(err);
  });
}

function toGeojson (diffEl) {
  var properties = {};
  properties.id = diffEl.id;
  properties.timestamp = diffEl.timestamp;
  properties.changeset = diffEl.changeset;
  properties.user = diffEl.user;
  properties.tags = diffEl.tags;

  var geo = {
    'type': 'Feature',
    'geometry': {
      'type': 'LineString',
      'coordinates': []
    },
    'properties': properties
  };
  if (diffEl.action === 'create' || diffEl.action === 'modify') {
    var nodelist = diffEl.nodes.map(function (node) {
      return [node.lon, node.lat];
    });
    var first = nodelist[0];
    var last = nodelist[nodelist.length - 1];
    if (first[0] === last[0] && first[1] === last[1]) {
      geo.geometry.coordinates = [nodelist];
      geo.geometry.type = 'Polygon';
    } else {
      geo.geometry.coordinates = nodelist;
    }
  }
  return geo;
}
