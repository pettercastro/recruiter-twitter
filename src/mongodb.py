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


def get_cursor_followers_by_id(screen_name):
    return db.cursor_follower.find_one({'_id': screen_name})


def insert_or_update_cursor_followers(screen_name, cursor):
    if not get_cursor_followers_by_id(screen_name):
        db.cursor_follower.insert_one({
            '_id': screen_name,
            'current': cursor
        })
    else:
        db.cursor_follower.update(
            {'_id': screen_name},
            {'$set': {
                'current': cursor
            }}
        )


def insert_tweets(tweets):
    db.user_tweets.insert_many(tweets)


def get_max_id_by_id(screen_name):
    return db.max_id.find_one({'_id': screen_name})


def insert_or_update_max_id_tweet(screen_name, max_id):
    if not get_max_id_by_id(screen_name):
        db.max_id.insert_one({
            '_id': screen_name,
            'max_id': max_id
        })
    else:
        db.max_id.update(
            {'_id': screen_name},
            {'$set': {
                'max_id': max_id
            }}
        )


def get_network_follower(user_id, follower_id):
    return db.network_follower.find_one({'user_id': user_id, 'follower_id': follower_id})


def insert_follower_if_not_exists(user_id, follower_id):
    if not get_network_follower(user_id, follower_id):
        db.network_follower.insert_one({'user_id': user_id, 'follower_id': follower_id})


def save_error(data):
    db.errors.insert_one(data)
