/*  Preprocess timeline */
function preprocessTimeline (timelineArray) {
  // It's an array of Redis Strings
  var timeline = timelineArray.map(JSON.parse);
  return timeline.map(toGeojson);
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
      'coordinates': [],
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
