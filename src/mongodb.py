from pymongo import MongoClient


client = MongoClient()
db = client.recruiter_twitter
# db.followers_list.drop()
# db.cursor.drop()
# db.user_profile.drop()
# db.root.drop()
# db.network.drop()
# db.twitter_user.drop()
# db.cursor.drop()
# db.errors.drop()


def get_user_by_id(mid):
    return db.twitter_user.find_one({'_id': mid})


def insert_user(obj):
    db.twitter_user.insert_one(obj)


def get_all_users():
    return db.twitter_user.find()


def user_exists(mid):
    if get_user_by_id(mid):
        return True
    return False


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


def save_tweets(tweets):
    db.user_tweets.insert_many(tweets)


def get_tweet_by_id(mid):
    return db.user_tweets.find_one({'_id': mid})


def get_user_tweets_processed(mid):
    return db.user_tweets_processed.find_one({'_id': mid})


def insert_user_tweets_processed(mid):
    db.user_tweets_processed.insert_one({'_id': mid})


def save_error(data):
    db.errors.insert_one(data)
