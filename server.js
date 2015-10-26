var express = require('express');
var app = express();
var server = require('http').Server(app);
// var io = require('socket.io')(server);
var path = require('path');
var Redis = require('ioredis');
var compress = require('compression');
var cors = require('cors');

var redis = new Redis();
server.listen(8080);

app.use(cors());
app.use(compress());
app.use(express.static(path.join(__dirname, 'static')));

// Timeline route
app.get('/timeline', function (req, res) {
  redis.lrange('ogp:timeline', 0, 1000).then(function (result) {
    res.send(result);
  });
});

// Buildings route
app.get('/buildings', function (req, res) {
  redis.zrevrange('ogp:buildings', 0, 10, 'WITHSCORES').then(function (result) {
    res.send(result);
  });
});

// Waterways route
app.get('/waterways', function (req, res) {
  redis.zrevrange('ogp:waterways', 0, 10, 'WITHSCORES').then(function (result) {
    res.send(result);
  });
});

// Highways route
app.get('/highways', function (req, res) {
  redis.zrevrange('ogp:highways', 0, 10, 'WITHSCORES').then(function (result) {
    res.send(result);
  });
});

// Changes route
app.get('/changes', function (req, res) {
  redis.zrevrange('ogp:changes', 0, 10, 'WITHSCORES').then(function (result) {
    res.send(result);
  });
});

// User route
app.get('/users/:user', function (req, res, next) {
  var user = req.params.user;
  redis.lrange('ogp:timeline:' + user, 0, 1000).then(function (result) {
    res.send(result);
  });
});
