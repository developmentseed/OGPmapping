/*  Preprocess timeline */
function preprocessTimeline (timelineArray) {
  // It's an array of Redis Strings
  var timeline = timelineArray.map(JSON.parse);
  timeline.forEach(function (element) {
    console.log(element);
  });
}
