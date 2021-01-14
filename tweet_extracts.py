import pandas as pd
import numpy as np
import searchtweets
from searchtweets import load_credentials, gen_rule_payload, ResultStream,collect_results
import json
import yaml
from datetime import date
import json_lines


def extract_tweets():
    today = date.today()
    d1 = today.strftime("%d-%m-%Y")

    with open('config.json','r') as f:
        keys = json.load(f)

    config = dict(
        search_tweets_api = dict(
            account_type = 'premium',
            endpoint = 'https://api.twitter.com/1.1/tweets/search/30day/development1.json',
            consumer_key = keys['consumer_key'],
            consumer_secret = keys['consumer_secret'])
            )
    with open('twitter_keys_fullhistory.yaml', 'w') as config_file:
        yaml.dump(config, config_file, default_flow_style=False)

    premium_search_args = load_credentials("twitter_keys_fullhistory.yaml",
                                        yaml_key="search_tweets_api",
                                        env_overwrite=False)

    SEARCH_QUERY = 'to:Lloydsbank'
    RESULTS_PER_CALL = 100
    FROM_DATE = "2020-06-01"
    TO_DATE = "2020-06-10"
    MAX_RESULTS = 100000
    FILENAME = 'twitter_input_data_{}_{}.jsonl'.format(FROM_DATE, TO_DATE)  # Where the Tweets should be saved
    PRINT_AFTER_X = 100


    rule = gen_rule_payload(SEARCH_QUERY,
                            results_per_call=RESULTS_PER_CALL,
                            from_date=FROM_DATE,
                            to_date=TO_DATE
                            )

    rs = ResultStream(rule_payload=rule,
                    max_results=MAX_RESULTS,
                    **premium_search_args)

    with open(FILENAME, 'a', encoding='utf-8') as f:
        n = 0
        for tweet in rs.stream():
            n += 1
            if n % PRINT_AFTER_X == 0:
                print('{0}: {1}'.format(str(n), tweet['created_at']))
            json.dump(tweet, f)
            f.write('\n')


    new_tweets = []
    dates_created = []
    location = []
    user= []

    with open(FILENAME, 'rb') as f:
        for item in json_lines.reader(f):
            try:
                new_tweets.append(item['extended_tweet']['full_text'])
            except KeyError as e:
                new_tweets.append(item['text'])
                dates_created.append(item['created_at'])
                location.append(item['user']['location'])
                user.append(item['user']['id'])

    dataframe = pd.DataFrame(list(zip(user, location, dates_created, new_tweets)), 
                columns =['User', 'Location', 'date_created', 'text'])
    print(dataframe.head())
    dataframe.to_csv("tweets.csv", sep =",")

if __name__ == "__main__":
    extract_tweets()
    print("finished")

