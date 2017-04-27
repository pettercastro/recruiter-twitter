from twitter import Twitter
import mongodb as db


def save_followers(screen_name):

    cursor = db.get_cursor_followers_by_id(screen_name)
    if cursor and cursor['current'] != 0:
        current_c = cursor['current']
    else:
        current_c = -1

    batch = twitter.request_cursor(
        'followers/ids',
        {
            'screen_name': screen_name,
            'cursor': current_c,
            'count': 5000,
            'stringify_ids': True
        }
    )

    for data in batch:
        db.insert_or_update_cursor_followers(screen_name, current_c)

        for item in data['ids']:
            user = db.get_user_by_id(data['ids'])
            if user:
                db.insert_follower_if_not_exists(screen_name, user['screen_name'])

        current_c = data['next_cursor']

    db.insert_or_update_cursor_followers(screen_name, 0)


twitter = Twitter('../twitter.cfg')

users_profiles = db.get_all_users()
for user in users_profiles:
    save_followers(user['_id'])
