import pandas as pd
from mastodon import Mastodon, MastodonNotFoundError, MastodonRatelimitError, StreamListener
from glob import glob
from bs4 import BeautifulSoup

m = Mastodon(
        api_base_url=f'https://mastodon.au',
        # access_token=os.environ['MASTODON_ACCESS_TOKEN']
        access_token='iXt13zlO1M45nKwwUIXO7-90sOZLawvCnL5YXlQizdI'
    )


class Listener(StreamListener):
    def __init__(self, **parameters):
        self.old_file_num = None
        self.output_path = None
        self.df_list = []
        self.load_param(**parameters)
        self.this_count = 0

    def load_param(self, **parameters):
        self.output_path = parameters['file_path']
        self.old_file_num = len(glob(self.output_path + '/*.csv'))

    def on_update(self, status):
        # test = json.dumps(status, indent=2, sort_keys=True, default=str)
        # print(status)
        language = status['language']
        usr_id = status['account']['id']
        create_time = status['created_at']
        tags = ','.join([k['name'] for k in status['tags']])
        toot = status['content']
        soup = BeautifulSoup(toot, "lxml")
        # print(toot)
        content = ' '.join([k.text for k in soup.find_all('p')])
        self.df_list.append({'usr_id': usr_id, 'create_time': create_time, 'toot': content, 'tag': tags, 'language': language})
        if len(self.df_list) >= 500:
            sub_df = pd.DataFrame(self.df_list)
            sub_df.to_csv(self.output_path + '/mastodon_au_{}.csv'.format(self.old_file_num), index=False)
            self.df_list = []
            self.old_file_num += 1
            self.this_count += 10
            print('{} toots have been harvested in this run'.format(self.this_count))
        # print('------------------')
        # df_list.append({'usr_id': usr_id, 'create_time': create_time, 'toot': toot, 'tag': tags, 'language': language})


df_list = []
parameter = {
    'file_path': '/Users/vonno/PythonProject/ass2/mastodon.au.data'
}
harvester = Listener(**parameter)
m.stream_public(harvester)




