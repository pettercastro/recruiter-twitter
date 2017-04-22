from twitter import Twitter
import time
from datetime import datetime
import mongodb as db


twitter = Twitter('../twitter.cfg')


def read_root_accounts(filename):
    with open(filename, 'r') as file:
        for line in file:
            yield line


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


def get_followers_ids(screen_name):
    return twitter.request_cursor(
        'followers/ids',
        {'screen_name': screen_name, 'stringify_ids': True, 'count': 5000}
    )


def get_followers(screen_name):
    cursor = db.get_cursor_by_id(screen_name)
    if cursor:
        current_c = cursor['current']
    else:
        current_c = -1

    batch = twitter.request_cursor(
        'followers/list',
        {'screen_name': screen_name, 'cursor': current_c}
    )

    for data in batch:
        db.insert_or_update_cursor(screen_name, current_c)

        for item in data['users']:
            yield item

        current_c = data['next_cursor']


def save_profiles(screen_name):
    obj = db.get_root_by_id(screen_name)
    if not obj:
        for user in get_followers(screen_name):
            print('Getting user {}'.format(user['screen_name']))
            is_valid = valid_user(user)
            if is_valid:
                db.save_if_not_exists(user)

        db.insert_root(screen_name)


accounts = read_root_accounts('../data/root_accounts.txt')
for root_account in accounts:
    print('Saving followers profiles for root acount {}'.format(root_account))
    save_profiles(root_account)

print('DONE!')
