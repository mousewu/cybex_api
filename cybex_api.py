"""A client to interact with node and to save data to mongo."""

from pymongo import MongoClient
import crawler_util
import requests
import json
import sys
import os
import logging
import time
import tqdm
import urllib.parse
from datetime import datetime
sys.path.append(os.path.realpath(os.path.dirname(__file__)))


LOGFIL = "crawler.log"

logging.basicConfig(filename=LOGFIL, level=logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.WARNING)

class Cybex_Api(object):
    def __init__(self,url = "https://hongkong.cybex.io/",delay=0.001):
        logging.debug("Starting Crawler")
        self.url = url
        self.headers = {"content-type": "application/json"}
        self.mongo_client = crawler_util.initMongo(MongoClient('mongodb://longhashdba:longhash123'+urllib.parse.quote('!@#')+'QAZ@localhost/parity'),"id")
        self.delay = delay

    def _rpcRequest(self, method, params, key):
        payload = {
            "method": method,
            "params": params,
            "jsonrpc": "2.0",
            "id": 1
        }
        time.sleep(self.delay)
        res = requests.post(
              self.url,
              data=json.dumps(payload),
              headers=self.headers).json()
        # print(res)
        return res[key]

    def get_account_count(self):
        result = self._rpcRequest('get_account_count',[],'result')
        return result

    def get_accounts(self,accounts):
        result = self._rpcRequest('get_accounts',[accounts],'result')
        return result

    def parse_get_accounts(self):
        total_no = self.get_account_count()
        for i in tqdm.tqdm(range(int(total_no / 100))):
            dd = list(range(i * 100, (i + 1) * 100))
            accounts = ['1.2.' + str(x) for x in dd]
            result = self.get_accounts(accounts)
            #e = crawler_util.insertMongo(self.mongo_client, result)
            print(result)


if __name__ == "__main__":
    cybex_api = Cybex_Api()
    cybex_api.parse_get_accounts()
