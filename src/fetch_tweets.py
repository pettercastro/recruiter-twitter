from twitter import Twitter
import mongodb as db


enc = lambda x: x.encode('utf-8', errors='ignore')

twitter = Twitter('../twitter.cfg')


def save_tweets(screen_name):
    if not db.get_user_tweets_processed(screen_name):
        tweets = twitter.request_paginated(
            'statuses/user_timeline',
            {'screen_name': screen_name, 'count': 200}
        )

        tweets_array = []
        for i, tweet in enumerate(tweets):
            if not db.get_tweet_by_id(tweet['id_str']):
                tweet['screen_name'] = screen_name
                tweets_array.append(tweet)

            # End loop if we reach the max allowed number of tweets
            if i >= 1000:
                break

        db.insert_user_tweets_processed(screen_name)
        db.save_tweets(tweets_array)


users_profiles = db.get_all()
for user in users_profiles:
    save_tweets(user['screen_name'])
