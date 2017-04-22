from pymongo import MongoClient
import itertools


client = MongoClient()
db = client.twitter_data

# db.followers_list.drop()
# db.cursor.drop()
# db.user_profile.drop()
# db.root.drop()
# db.network.drop()


def get_root_by_id(screen_name):
    return db.root.find_one({'_id': screen_name})


def insert_root(screen_name):
    db.root.insert_one({'_id': screen_name})


def get_by_id(mid):
    return db.user_profile.find_one({'_id': mid})


def insert(obj):
    db.user_profile.insert_one(obj)


def get_all():
    return db.user_profile.find()


def get_cursor_by_id(screen_name):
    return db.cursor.find_one({'_id': screen_name})


def insert_or_update_cursor(screen_name, cursor):
    if not get_cursor_by_id(screen_name):
        db.cursor.insert_one({
            '_id': screen_name,
            'current': cursor
        })
    else:
        db.cursor.update(
            {'_id': screen_name},
            {'$set': {
                'current': cursor
            }}
        )


def save_if_not_exists(user):
    if not get_by_id(user['screen_name']):
        user['_id'] = user['screen_name']
        insert(user)


def save_tweets(tweets):
    db.user_tweets.insert_many(tweets)


def get_tweet_by_id(mid):
    return db.user_tweets.find_one({'id_str': mid})


def get_user_tweets_processed(screen_name):
    return db.user_tweets_processed.find_one({'_id': screen_name})


def insert_user_tweets_processed(screen_name):
    db.user_tweets_processed.insert_one({'_id': screen_name})
