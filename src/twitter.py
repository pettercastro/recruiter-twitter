from TwitterAPI import TwitterAPI, TwitterRestPager
from TwitterAPI.TwitterError import TwitterConnectionError
import configparser
import time


class Twitter(object):

    def __init__(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        self.twitter = TwitterAPI(
            config.get('twitter', 'consumer_key'),
            config.get('twitter', 'consumer_secret'),
            auth_type='oAuth2'
        )

    def request(self, resource, params, max_tries=5):
        for i in range(max_tries):
            try:
                response = self.twitter.request(resource, params)
                if response.status_code == 200:
                    return response
                elif response.status_code in [429, 503]:
                    print('Got error: {}\nSleeping for 15 minutes.'.format(response.text))
                    time.sleep(61 * 15)
                else:
                    print(response)
                    raise ValueError(response.json())
            except TwitterConnectionError as e:
                print('Got a connection error: {}\nSleeping for 15 minutes.'.format(resource))
                time.sleep(61 * 15)


    def request_paginated(self, resource, params):
        response = TwitterRestPager(self.twitter, resource, params)
        for item in response.get_iterator():
            if 'text' in item:
                yield item
            elif 'message' in item and item['code'] in [88, 429, 503]:
                print('Got error: {}\nSleeping for 15 minutes.'.format(item['message']))
                time.sleep(61 * 15)
            else:
                print(item)
                raise ValueError(item)

    def request_cursor(self, resource, params):
        if 'cursor' not in params:
            params['cursor'] = -1
        if 'count' not in params:
            params['count'] = 200
        while True:
            response = self.request(resource, params)
            data = response.json()
            yield data
            if data['next_cursor'] == 0:
                break
            params['cursor'] = data['next_cursor']
