/*global L, $, io, omnivore, tinysort */
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

// new L.Control.Zoom({ position: 'topright' }).addTo(map);

var nextTimeline = [];
var currentTimeline = [];
var colors = '0,1,2,3,4,5,6,7,8,9,10'.split(',');
var paused = false;
var progressBarWidth = 0;
var currentProgress = 0;

function reset () {
  $('#leaderboard').empty();
  $('#logroll').empty();
  $('#progress-bar').css('width', '0%');

  currentTimeline = nextTimeline.slice(0);
  progressBarWidth = currentTimeline.length;
  currentTimeline.unshift('LAST');
  currentProgress = 0;

  // Reinitialize color pool
  colors = '0,1,2,3,4,5,6,7,8,9,10'.split(',');
}

$.get(root + '/timeline', function (timeline) {
  nextTimeline = JSON.parse(timeline);
  reset();
  $('#spinner').hide();
  setInterval(function () {
    if (!paused) {
      render(currentTimeline.pop());
    }
  }, 400);
});

var options = {
  lng: function (d) { return d[0]; },
  lat: function (d) { return d[1]; },
  duration: 2000
};
var pingLayer = L.pingLayer(options).addTo(map);
pingLayer.radiusScale().range([2, 18]);
pingLayer.opacityScale().range([1, 0]);

var colorMap = {
  '0': '#ffffff',
  '1': '#8dd3c7',
  '2': '#F2E855',
  '3': '#bebada',
  '4': '#F2695A',
  '5': '#6CB7EB',
  '6': '#fdb462',
  '7': '#b3de69',
  '8': '#FA89B1',
  '9': '#d9d9d9',
  '10': '#bc80bd'
};

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
  var leaderboard = $('#leaderboard');

  var timecode = new Date(Date.parse(element.time));
  var date = timecode.getHours() + ':' + timecode.getMinutes();

  var center = omnivore.wkt.parse(element.last).getBounds().getCenter();

  currentProgress += 1;
  $('#progress-bar').css('width', (100 * currentProgress / progressBarWidth) + '%');

  var el;
  if ($('[tag=' + element.hashtag + ']').length === 0) {
    el = $('<li>' + '#' + element.hashtag + '</li>');
    $(el).attr('tag', element.hashtag);
    $(el).attr('count', element.count);
    $(el).attr('leader', 'no');
    $(el).attr('color', '0');
    leaderboard.append(el);

  } else {
    el = $('[tag=' + element.hashtag + ']');
    var count = Number($(el).attr('count'));
    $(el).attr('count', count + element.count);
  }
  calculateRank();
  giveColors();
  takeColors();
  reColor();
  sort();

  // The new color
  var color = $(el).attr('color');

  pingLayer.ping([center.lng, center.lat], 'color_' + color);
  logroll.prepend('<div class="logroll-item"><i>' +
                  date + '</i> - ' +
                  element.count + ' ' + element.feature + '(s) -' +
                  '<span style="color: ' + colorMap[color] + ';">' +
                  element.hashtag + '<span>' + '</div>');

  if (logroll.children().length > 100) {
    $('#logroll div:last-child').remove();
  }
}

function reColor () {
  $('#leaderboard').children().each(function () {
    $(this).css('color', colorMap[$(this).attr('color')]);
  });
}

function takeColors () {
  $('.need-color').each(function () {
    var item = $(this);
    var color = colors.pop();
    item.attr('color', color);
    item.removeClass('need-color');
  });
}

function calculateRank () {
  var ranks = [];
  $('#leaderboard').children().each(function () {
    ranks.push($(this));
  });
  ranks.sort(function (a, b) {
    var a_count = Number(a.attr('count'));
    var b_count = Number(b.attr('count'));
    if (a_count < b_count) return 1;
    if (a_count > b_count) return -1;
    if (a_count === b_count) return 0;
  }).forEach(function (el, index) {
    el.attr('rank', index + 1);
  });
}

function giveColors () {
  $('#leaderboard').children().each(function () {
    var item = $(this);
    var isLeader = (item.attr('leader') === 'yes');
    var isTopTen = Number(item.attr('rank')) <= 10;
    var currentColor = item.attr('color');

    // give back color
    if (isLeader && !isTopTen) {
      colors.push(currentColor);
      item.attr('leader', 'no');
      item.attr('color', '0');
    }

    if (!isLeader && isTopTen) {
      item.addClass('need-color');
      item.attr('leader', 'yes');
    }
  });
}

function sort () {
  var ul = document.getElementById('leaderboard');
  var lis = ul.querySelectorAll('li');
  var liHeight = lis[0].offsetHeight;

  ul.style.height = ul.offsetHeight + 'px';
  for (var i = 0, l = lis.length; i < l; i++) {
    var li = lis[i];
    li.style.position = 'absolute';
    li.style.top = i * liHeight + 'px';
  }
  tinysort('ul#leaderboard>li', {attr: 'count', order: 'desc'}).forEach(function (elm, i) {
    setTimeout((function (elm, i) {
      elm.style.top = i * liHeight + 'px';
      var $elm = $(elm);
      if (Number($elm.attr('rank')) < 10) { $elm.show(); }
      else $elm.hide();
    }).bind(null, elm, i), 40);
  });
}
