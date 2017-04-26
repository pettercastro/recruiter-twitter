from twitter import Twitter
import time
from datetime import datetime
import mongodb as db


MAX_NUMBER_FOLLOWERS = 20000


twitter = Twitter('../twitter.cfg')


def read_root_accounts(filename):
    with open(filename, 'r') as file:
        for line in file:
            yield line


def save_user(user):
    new_user = {}
    new_user['_id'] = user['screen_name']
    new_user['id_str'] = user['id_str']
    new_user['name'] = user['name']
    new_user['location'] = user['location']
    new_user['description'] = user['description']
    new_user['followers_count'] = user['followers_count']
    new_user['friends_count'] = user['friends_count']
    new_user['time_zone'] = user['time_zone']
    new_user['geo_enabled'] = user['geo_enabled']
    new_user['statuses_count'] = user['statuses_count']
    new_user['lang'] = user['lang']
    new_user['date_last_tweet'] = user['status']['created_at']
    db.insert_user(new_user)


def get_diff_between_twitter_date_and_today(date):
    date_format = '%Y-%m-%d'
    a = datetime.strptime(date, '%a %b %d %H:%M:%S +0000 %Y').date()
    b = datetime.strptime(time.strftime(date_format), date_format).date()
    return b - a


def valid_user(user):
    if user['lang'] not in ['en', 'es', 'en-gb']:
        return False
    if user['protected']:
        return False
    if user['statuses_count'] < 5:
        return False
    if 'status' not in user:
        return False
    td = get_diff_between_twitter_date_and_today(user['status']['created_at'])
    if td.days > 150:
        return False
    return True


def get_followers(screen_name):

    cursor = db.get_cursor_by_id(screen_name)
    if cursor and cursor['current'] != 0:
        current_c = cursor['current']
    else:
        current_c = -1

    batch = twitter.request_cursor(
        'followers/list',
        {'screen_name': screen_name, 'cursor': current_c, 'count': 200}
    )

    for data in batch:
        db.insert_or_update_cursor(screen_name, current_c)

        all_users_exist = True
        for item in data['users']:
            user_exist = yield item
            if not user_exist:
                all_users_exist = False

        if all_users_exist:
            break

        current_c = data['next_cursor']

    db.insert_or_update_cursor(screen_name, 0)


def save_profiles(screen_name, max_followers):
    followers = get_followers(screen_name)
    cont = 1
    for user in followers:
        try:
            print('Getting user {}'.format(user['screen_name']))
            is_valid = valid_user(user)
            generator_param = True
            if is_valid:
                user_exists = db.user_exists(user['screen_name'])
                generator_param = user_exists
                if not user_exists:
                    save_user(user)

                if cont > max_followers:
                    return

                cont += 1

            followers.send(generator_param)

        except Exception as e:
            db.save_error({'data': user['screen_name'], 'error': str(e)})


accounts = read_root_accounts('../data/root_accounts.txt')
for root_account in accounts:
    print('Saving followers profiles for root acount {}'.format(root_account))
    save_profiles(root_account, MAX_NUMBER_FOLLOWERS)

print('DONE!')
