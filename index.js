var express = require('express');
var app = express();
var server = require('http').Server(app);
var io = require('socket.io')(server);
var path = require('path');
var Redis = require('ioredis');

var pubsub = new Redis();
var redis = new Redis();
server.listen(8080);

app.use(express.static(path.join(__dirname, 'static')));

app.get('/hashtags/:hashtag', function (req, res, next) {
  var hashtag = req.params.hashtag;
  redis.lrange('hashtags:list:' + decodeURIComponent(hashtag), 0, 100).then(function (result) {
    res.send(result);
  });
});

app.get('/timeline', function (req, res) {
  redis.get('timeline').then(function (result) {
    res.send(result);
  });
});

io.on('connection', function (socket) {
  redis.get('timeline').then(function (result) {
    socket.emit('timeline', result);
  });
});

pubsub.subscribe('hashtagsch', function (err) {
  if (err) console.log(err);
});

pubsub.on('message', function (channel, data) {
  if (channel === 'hashtagsch') {
    redis.get('timeline').then(function (result) {
      io.emit('timeline', result);
    });
  }
});

