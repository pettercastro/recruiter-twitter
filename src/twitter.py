from TwitterAPI import TwitterAPI
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
            except TwitterConnectionError:
                print('Got a connection error: {}\nSleeping for 15 minutes.'.format(resource))
                time.sleep(61 * 15)

    def request_timeline(self, resource, params):
        while True:
            response = self.request(resource, params)
            data = response.json()
            if not data:
                break

            mid = None
            batch = []
            for item in response.get_iterator():
                mid = item['id']
                batch.append(item)

            yield batch
            params['max_id'] = mid - 1

    def request_cursor(self, resource, params):
        while True:
            response = self.request(resource, params)
            data = response.json()
            yield data
            if data['next_cursor'] == 0:
                break
            params['cursor'] = data['next_cursor']
