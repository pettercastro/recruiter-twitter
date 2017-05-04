from twitter import Twitter
import mongodb as db

# enc = lambda x: x.encode('utf-8', errors='ignore')
MAX_NUMBER_TWEETS = 1000


twitter = Twitter('../twitter.cfg')


def save_tweets(screen_name, max_tweets):
    count = 200
    max_id = db.get_max_id_by_id(screen_name)
    if max_id and max_id['max_id'] == 0:
        return

    try:
        if not max_id:
            batch = twitter.request_timeline(
                'statuses/user_timeline',
                {'screen_name': screen_name, 'count': count, 'trim_user': 1}
            )
        else:
            batch = twitter.request_timeline(
                'statuses/user_timeline',
                {'screen_name': screen_name, 'count': count, 'max_id': max_id['max_id'], 'trim_user': 1}
            )

        for i, tweets in enumerate(batch):
            number_tweets_saved = (i + 1) * count
            max_id = None
            for tweet in tweets:
                max_id = tweet['id']
                tweet['screen_name'] = screen_name

            db.insert_or_update_max_id_tweet(screen_name, max_id)
            db.insert_tweets(tweets)

            # End loop if we reach the max allowed number of tweets
            if number_tweets_saved >= max_tweets:
                break

        db.insert_or_update_max_id_tweet(screen_name, 0)

    except Exception as err:
        db.save_error_tweets({'data': screen_name, 'error': str(err)})

    


users_profiles = db.get_all_users()
for user in users_profiles:
    print('Getting tweets for {}'.format(user['_id']))
    save_tweets(user['_id'], MAX_NUMBER_TWEETS)
