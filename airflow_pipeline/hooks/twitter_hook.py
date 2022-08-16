# from airflow.hooks.http_hook import HttpHook
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import requests
import json

class TwitterHook(HttpHook):

    def __init__(self, query, conn_id = None, start_time = None, end_time = None):
        self.query = query
        self.conn_id = conn_id or "twitter_default"
        self.start_time = start_time
        self.end_time = end_time
        super().__init__(http_conn_id=self.conn_id)

    # Montando URL - create_url
    def create_url(self):        
        query = self.query        
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,text"        
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
        start_time = (f"&start_time={self.start_time}" if self.start_time else "")
        end_time = (f"&end_time={self.end_time}" if self.end_time else "")        
        url = "{}/2/tweets/search/recent?query={}&{}&{}{}{}".format(self.base_url, query, tweet_fields, user_fields, start_time, end_time)

        return url

    # Montando Headers - connect_to_endpoint
    def connect_to_endpoint(self, url, session):
        response = requests.Request("GET", url)
        prep = session.prepare_request(response)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})

    # recriando a URL com o Token - paginate
    def paginate(self, url_raw, session):
        list_json_response = []

        response = self.connect_to_endpoint(url_raw, session)

        # leitura do json
        json_response = response.json()
        list_json_response.append(json_response)

        while "next_token" in json_response.get("meta", {}):
            next_token = json_response['meta']['next_token']
            nova_url = f"{url_raw}&next_token={next_token}"
            response = self.connect_to_endpoint(nova_url, session)
            json_response = response.json()
            list_json_response.append(json_response)

        return list_json_response

    def run(self): 
        session = self.get_conn()        
        url_raw = self.create_url()
        
        return self.paginate(url_raw, session)

if __name__ == "__main__":
    
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)

    for pg in TwitterHook(query="NBABrasil", start_time=start_time, end_time=end_time).run():
        print(json.dumps(pg, indent=4, sort_keys=True))