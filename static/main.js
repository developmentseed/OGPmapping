/*global L, $, preprocessTimeline*/
var root = 'http://45.55.146.128:8080';
var mapboxTiles = L.tileLayer('https://api.mapbox.com/v4/devseed.07f51987/{z}/{x}/{y}.png?access_token=pk.eyJ1IjoiZGV2c2VlZCIsImEiOiJnUi1mbkVvIn0.018aLhX0Mb0tdtaT2QNe2Q', {
//    maxZoom: 2,
    minZoom: 2,
    attribution: "© <a href='https://www.mapbox.com/map-feedback/'>Mapbox</a> © <a href='http://www.openstreetmap.org/copyright'>OpenStreetMap contributors</a>"
});

var map = L.map('map', { zoomControl: false })
    .addLayer(mapboxTiles)
    .setView([18.025966, -5], 2)
    .setMaxBounds([ [89, -180], [-89, 180] ])
    ;

var geojsonLayer = L.geoJson().addTo(map);

var nextTimeline = [];
var currentTimeline = [];
var paused = false;
var progressBarWidth = 0;
var currentProgress = 0;

function reset () {
  $('#leaderboard').empty();
  $('#logroll').empty();
  $('#progress-bar').css('width', '0%');

  currentTimeline = nextTimeline;
  progressBarWidth = currentTimeline.length;
  currentTimeline.unshift('LAST');
  currentProgress = 0;
}

$.get(root + '/timeline', function (timeline) {
  nextTimeline = preprocessTimeline(timeline);
  reset();
  $('#spinner').hide();
  setInterval(function () {
    if (!paused) {
      render(currentTimeline.pop());
    }
  }, 3000);
});

function render (element) {
  if (element === 'LAST') {
    paused = true;
    setTimeout(function () {
      paused = false;
      reset();
    }, 3000);
    return;
  }

  var logroll = $('#logroll');
  // var leaderboard = $('#leaderboard');

  var timecode = new Date(Date.parse(element.properties.created_at));
  var date = timecode.getHours() + ':' + timecode.getMinutes();
  geojsonLayer.clearLayers();
  geojsonLayer.addData(element);
  map.fitBounds(geojsonLayer.getBounds());

  currentProgress += 1;
  $('#progress-bar').css('width', (100 * currentProgress / progressBarWidth) + '%');

  logroll.prepend('<div class="logroll-item"><i>' +
                  date + '</i> - ' +
                  element.properties.user + '</div>');

  if (logroll.children().length > 100) {
    $('#logroll div:last-child').remove();
  }
}

