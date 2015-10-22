import sys
import json
from itertools import tee, izip
from shapely.geometry import Point, LineString, Polygon
from shapely import wkt
import redis

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

def removeCommonHashtagMistakes(hashtag):
    # Check if first letter is a hash
    if (hashtag[0] == '#'):
        hashtag = hashtag[1:]

    # Check if last letter is a comma
    if (hashtag[-1] == ','):
        hashtag = hashtag[:-1]

    return hashtag

def addHashtags(obj):
    s = obj['metadata']['comment']
    hashtag_set = list(set(part[1:] for part in s.split() if part.startswith('#')))
    notNull = filter(lambda hashtag: len(hashtag) > 0, hashtag_set)
    obj['hashtags'] = [removeCommonHashtagMistakes(hashtag) for hashtag in notNull]
    return obj

def has_tag(tag):
    return lambda obj: tag in obj['tags']

def either(lambda1, lambda2):
    return lambda arg: lambda1(arg) or lambda2(arg)

def processFeature(obj):
    nodes = obj['nodes']
    nodelist = []
    feature = {}
    for ref in nodes:
        if ('lon' in ref and 'lat' in ref):
            nodelist.append(( float(ref['lon']), float(ref['lat'])))
    try:
        if has_tag('building')(obj):
            feature = Polygon(nodelist)
        else:
            feature = LineString(nodelist)

        return {'user': obj['user'],
                'id': obj['id'],
                'changeset': obj['metadata']['id'],
                'date': obj['metadata']['created_at'],
                'feature': wkt.dumps(feature),
                'action': obj['action'],
                'comment': obj['metadata']['comment'],
                'hashtags': obj['hashtags']
                }
    except Exception as e:
        print e
        print nodelist
        return None

def ensureComment(obj):
    if 'comment' not in obj['metadata']:
        obj['metadata']['comment'] = '(no comment)'

    return obj

def outputHashtags(partition):
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    pipe = r.pipeline()
    for record in partition:
        pipe.lpush('hashtags:list:' + record[1], record[0][0] + '|' + record[0][1])
        pipe.publish('hashtagsch', record)
    pipe.execute()

def outputTrending(partition, time):
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    pipe = r.pipeline()
    for record in partition:
        pipe.set('hashtags:score:' + time + ':' + record[0], record[1])
        pipe.publish('hashtagsch', record)
    pipe.execute()
    
def outputFeatures(partition):
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    pipe = r.pipeline()
    for record in partition:
        rec = json.dumps(record)
        pipe.lpush('features', rec)
        pipe.publish('featuresch', rec)
    pipe.ltrim('features', 0, 1000)
    pipe.execute()

def createContext(checkpoint):
    sc = SparkContext(master="local[*]", appName="PlanetStreamHashtags2")
    ssc = StreamingContext(sc, 30)

    appName = "PlanetStreamHashtags2"
    streamName = "test"
    endpointUrl = "https://kinesis.us-west-1.amazonaws.com"
    regionName = "us-west-1"
    lines = KinesisUtils.createStream(
        ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 60,
        decoder=lambda obj:obj)
    relevantLines = (lines.map(lambda line: json.loads(line))
            .filter(lambda obj: obj['type'] == 'way')
            .map(ensureComment)
            .map(addHashtags)
            )

    features = (relevantLines
            .filter(either(has_tag('building'), has_tag('highway')))
            .map(processFeature)
            .filter(lambda obj: obj is not None))

    features.foreachRDD(lambda rdd: rdd.foreachPartition(outputFeatures))

    hashtagFeatures = features.flatMap(lambda obj: [( (obj['feature'], obj['date']), hashtag) for hashtag in obj['hashtags']])
    hashtagFeatures.foreachRDD(lambda rdd: rdd.foreachPartition(outputHashtags))

    hashtagFeatures.pprint()

    hashtags6 = (
        features.flatMap(lambda obj: [(hashtag,1) for hashtag in obj['hashtags']])
        .reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 6 * 3600, 30)
        )
    hashtags6.pprint()


    hashtags6.foreachRDD(lambda rdd: rdd.foreachPartition(lambda partition: outputTrending(partition, '6')))

    ssc.checkpoint(checkpoint)
    return ssc

if __name__ == "__main__":
    checkpoint = "checkpoint" 
    ssc = StreamingContext.getOrCreate(checkpoint, lambda: createContext(checkpoint))
    ssc.start()
    ssc.awaitTermination()
