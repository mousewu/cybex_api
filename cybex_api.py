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
    def __init__(self,url = "https://apihk.cybex.io",delay=0.5):
        logging.debug("Starting Crawler")
        self.url = url
        self.headers = {"content-type": "application/json"}
        self.mongo_client = crawler_util.initMongo(MongoClient('mongodb://root:longhash123'+urllib.parse.quote('!@#')+'QAZ@localhost/admin'),"id")
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
        #print(res)
        return res[key]

    def get_account_count(self):
        result = self._rpcRequest('get_account_count',[],'result')
        return result

    def get_accounts(self,accounts):
        result = self._rpcRequest('get_accounts',[accounts],'result')
        return result

    def parse_get_accounts(self,all):
        if all:
            total_no = self.get_account_count()
            for i in tqdm.tqdm(range(int(total_no / 100))):
                dd = list(range(i * 100, (i + 1) * 100))
                accounts = ['1.2.' + str(x) for x in dd]
                result = self.get_accounts(accounts)
                e = crawler_util.insertMongo(self.mongo_client, result)
                #print(result)
        else:
            dd = list(range(57800,57890))
            accounts = ['1.2.' + str(x) for x in dd]
            result = self.get_accounts(accounts)
            print(result)
            e = crawler_util.insertMongo(self.mongo_client, result)

    def get_account_balances(self,asset,accounts):
        result =self._rpcRequest('get_account_balances',[accounts,asset],'result')
        return result

    def parse_account_balances(self,all,assets):
        if all:
            total_no = self.get_account_count()
            for i in tqdm.tqdm(range(total_no)):
                account = '1.2.' + str(i)
                result = self.get_account_balances(assets,account)
                for x in result:
                    x['id'] = account
                    x['time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                e = crawler_util.insertMongo(self.mongo_client, result)
                #print(result)
        else:
            dd = list(range(57800,57890))
            accounts = ['1.2.' + str(x) for x in dd]
            for account in accounts:
                result = self.get_account_balances(assets,account)
                for x in result:
                    x['id'] = account
                    x['time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                #print(result)
                e = crawler_util.insertMongo(self.mongo_client, result)



if __name__ == "__main__":
    cybex_api = Cybex_Api()
    #cybex_api.parse_get_accounts(False)
    cybex_api.parse_account_balances(True,["1.3.0","1.3.2","1.3.27","1.3.1384","1.3.1385"])
