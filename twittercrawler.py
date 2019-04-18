"""
Switches Twitter accounts and search tweets.

# -*- coding: utf-8 -*-
Created on Jan 21, 2019
Updated on Apr 16, 2019

@author: g-suzuki

TODO（追加予定）
- 開始・終了時刻を指定したサーチ機能
- リプライ・リツイート元のサーチ・ネットワーク構築機能
- フォロー・フォロワー関係追跡機能
"""


# import datetime
import time
import configparser as cp
import pandas as pd
import pickle as pkl
import csv
import os
from requests_oauthlib import OAuth1Session

import twitterapi


class TwitterCrawler:
    """
    複数アカウントを切り替えつつツイッターをクロールする.

    Attributes:
        search_type (str): "word"(キーワード検索)または"user"(ユーザ検索)
        keys (list): 検索するキーワード/ユーザのlist
        accountFile (str): 検索アカウントのAPIキーを書いたファイルのパス
        search_lang (str): 検索する言語（キーワード検索時のみ）．"ja"など
        twitterapis (dict): TwitterAPIクラスのインスタンスを格納したdict
        accounts (list): twitterインスタンス名のlist
        keystatuses (dict): 検索keyごとの検索状況が入ったdict
    """
    def __init__(self, search_type, keys=None,
                 account_file="./accounts.cfg",
                 search_lang="ja",
                 metadata_file="./crawl_metadata.pkl",
                 export_csv=True):
        """
        コンストラクタ. twitterアカウントを起動する.

        Args:
            search_type (str): "word"(キーワード検索)または"user"(ユーザ検索)
            keys (list): 検索するキーワード/ユーザのlist
            accountFile (str): 検索アカウントのAPIキーを書いたファイルのパス
            search_lang (str): 検索する言語（キーワード検索時のみ）．"ja"など
            metadata_file (str): 検索状況を記録したファイルがあれば、そのパス
            export_csv (bool): 結果をcsvに出力するか否か
        """
        self.search_type = search_type
        if keys:
            self.keys = keys
        else:
            self.keys = self.getSearchKeys()
        self.accountFile = account_file
        self.search_lang = search_lang
        self.twitterapis, self.accounts = self.makeClientInstance(export_csv)
        if os.path.exists(metadata_file):
            self.load_keystatus()
        else:
            self.keystatuses = self.makeKeyStatus()

    def getSearchKeys(self):
        """
        keywords.csv/keyusers.csvから検索keyを読み込む.

        Return:
            keys (list): 検索するキーワード/ユーザのlist
        """
        keys = []
        if self.search_type == "word":
            keyfile = "keywords.csv"
        else:
            keyfile = "keyusers.csv"

        with open(keyfile, 'r') as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')
            for row in reader:
                for k in row:
                    keys.append(k)

        if len(keys) == 0:
            print("No keys found.")

        return keys

    def makeClientInstance(self, export_csv=True):
        """
        accountFileの情報に基づきOAuth認証でツイッターインスタンスを作成する.

        Args:
            export_csv (bool): 結果をcsvに出力するか否か

        Return:
            twitterapis (dict): 検索を行うTwitterAPIクラスのdict
            accounts (list): アカウント名のlist
        """
        config = cp.ConfigParser()
        config.read(self.accountFile)
        # consumer_key = config.get("consumer", "key")
        # consumer_secret = config.get("consumer", "secret")

        accountDic = {}
        accounts = []
        for section in config.sections():
            if section == "consumer":
                continue

            accounts.append(section)

            accountDic[section] = {}
            accountDic[section]["screen_name"] = config.get(section,
                                                            "screen_name")
            accountDic[section]["access_token"] = config.get(section,
                                                             "access_key")
            accountDic[section]["access_secret"] = config.get(section,
                                                              "access_secret")
            # accountDic[section]["consumer_key"] = consumer_key
            # accountDic[section]["consumer_secret"] = consumer_secret
            accountDic[section]["consumer_key"] = config.get(section,
                                                             "consumer_key")
            accountDic[section]["consumer_secret"] = config.get(section,
                                                                "consumer_secret")

        twitterapis = {}
        for account in accounts:
            CK = accountDic[account]["consumer_key"]
            CS = accountDic[account]["consumer_secret"]
            AT = accountDic[account]["access_token"]
            AS = accountDic[account]["access_secret"]
            twitter = OAuth1Session(CK, CS, AT, AS)
            twitterapis[account] = twitterapi.TwitterAPI(account, twitter,
                                                         lang=self.search_lang,
                                                         word=None,
                                                         write_to_csv=export_csv)

        return twitterapis, accounts

    def makeKeyStatus(self):
        """
        検索keyごとの検索状況が入ったself.keystatusesを作成.

        Return:
            keystatuses (dict): 検索keyごとのツイートの取得状況が入ったdict
        """
        keystatuses = {}
        for k in self.keys:
            keystatuses[k] = {}
            keystatuses[k]["max_tw_id"] = None  # これまでクロールしたツイートの中で最新のid
            keystatuses[k]["max_tw_time"] = None
            keystatuses[k]["min_tw_id"] = None  # これまでクロールしたツイートの中で最古のid
            keystatuses[k]["min_tw_time"] = None
            keystatuses[k]["recent_min"] = None
            keystatuses[k]["since_tw_id"] = None  # 新たにツイートを取得する際、どこまで遡るか（つまり以前のクロール時の最新のid）
            keystatuses[k]["last_updated_time"] = None  # 最後にそのkeyで検索した時刻
            keystatuses[k]["total_crawled_num"] = 0
        return keystatuses

    def updateKeyStatus(self, t_api, key):
        """
        検索に使用したTwitterAPIクラスの情報からself.keystatusesを更新する.

        更新の仕方はケースに応じて異なる.

        Attrs:
            t_api :検索に使用したTwitterAPIインスタンス
            key (str or int): 検索key
        """
        if self.keystatuses[key]["since_tw_id"] is None:

            if t_api.crawled_num > 0:
                # Case1. 初回のクロールで取得ツイートあり（since, min, maxが全てNone）(maxとminを設定する)
                if (self.keystatuses[key]["max_tw_id"] is None) and \
                   (self.keystatuses[key]["min_tw_id"] is None):

                    self.keystatuses[key]["max_tw_id"] = t_api.crawled_max
                    self.keystatuses[key]["max_tw_time"] = t_api.crawled_max_t
                    self.keystatuses[key]["min_tw_id"] = t_api.crawled_min
                    self.keystatuses[key]["min_tw_time"] = t_api.crawled_min_t
                    self.keystatuses[key]["recent_min"] = t_api.crawled_min

                # Case2. 過去のpagingで取得ツイートあり（maxやminはNoneではなく，sinceはNone）（minを更新し、sinceはNoneのまま）
                else:

                    self.keystatuses[key]["min_tw_id"] = t_api.crawled_min
                    self.keystatuses[key]["min_tw_time"] = t_api.crawled_min_t
                    self.keystatuses[key]["recent_min"] = t_api.crawled_min

            # Case3. 取得ツイートなし(crawled_num==0)（minを更新しsinceにmaxの値を入れる，pagingなどを終了）
            if t_api.crawled_num == 0:

                if self.keystatuses[key]["max_tw_id"]:
                    max_id = self.keystatuses[key]["max_tw_id"]
                    self.keystatuses[key]["since_tw_id"] = max_id
                else:
                    msg = ('No tweets about "%s"found.'
                           ' You should delete this key.' % key)
                    print(msg)

        else:

            if t_api.crawled_num > 0:
                # Case4. paging後1回目のupdate（maxとsinceが同じ）（maxを更新し、minはそのままに）
                if self.keystatuses[key]["max_tw_id"] == \
                   self.keystatuses[key]["since_tw_id"]:

                    self.keystatuses[key]["max_tw_id"] = t_api.crawled_max
                    self.keystatuses[key]["max_tw_time"] = t_api.crawled_max_t
                    self.keystatuses[key]["recent_min"] = t_api.crawled_min

                # Case5. 1回目のupdateでpaging（max>since）（何もしない）
                else:

                    self.keystatuses[key]["recent_min"] = t_api.crawled_min

                # Case4/5の続き. pagingが終了した場合（取得ツイートのminが、sinceと同じ）（sinceにmaxの値を入れる）
                if self.keystatuses[key]["since_tw_id"] == t_api.crawled_min:

                    max_id = self.keystatuses[key]["max_tw_id"]
                    self.keystatuses[key]["since_tw_id"] = max_id

        self.keystatuses[key]["last_updated_time"] = t_api.updated_time
        self.keystatuses[key]["total_crawled_num"] += t_api.crawled_num

        return

    def save_keystatus(self, filename="./crawl_metadata.pkl"):
        """
        self.keystatusesをpickleに保存する.

        Args:
            filename (str): 保存先ファイルのパス.
        """
        with open(filename, mode='wb') as f:
            pkl.dump(self.keystatuses, f)

        return

    def load_keystatus(self, filename="./crawl_metadata.pkl"):
        """
        crawl_metadataを読み込む.

        Args:
            filename (str): 保存先ファイルのパス
        """
        with open(filename, mode='rb') as f:
            self.keystatuses = pkl.load(f)

        for k in self.keys:
            if k not in self.keystatuses.keys():
                self.keystatuses[k] = {}
                self.keystatuses[k]["max_tw_id"] = None
                self.keystatuses[k]["max_tw_time"] = None
                self.keystatuses[k]["min_tw_id"] = None
                self.keystatuses[k]["min_tw_time"] = None
                self.keystatuses[k]["recent_min"] = None
                self.keystatuses[k]["since_tw_id"] = None
                self.keystatuses[k]["last_updated_time"] = None
                self.keystatuses[k]["total_crawled_num"] = 0

    def selectClient(self):
        """
        clientStatusをもとに検索に使用するアカウントを決定.

        - API残機がある場合は一番残機が多いアカウント
        - API残機がない場合は一番復活が早いアカウント

        Return:
            selected_account (str):使用するアカウント
        """
        remainings = []
        resettimes = []
        for account in self.accounts:
            remaining = self.twitterapis[account] \
                            .clientStatus[self.search_type]["remaining_count"]
            reset_time = self.twitterapis[account] \
                             .clientStatus[self.search_type]["reset_time"]
            remainings.append(remaining)
            resettimes.append(reset_time)

        max_remaining = max(remainings)
        ## どれかしら制限がかかっていなければ、その中で最も残機が多いアカウントを使う
        if max_remaining > 0:
            idx = remainings.index(max_remaining)
            selected_account = self.accounts[idx]
        ## 全てのアカウントが制限がかかっている場合、１つアカウントが復活するまで待つ
        else:
            min_reset_time = min(resettimes)
            idx = resettimes.index(min_reset_time)
            selected_account = self.accounts[idx]
            now_time = int(time.time())
            wait_sec = min_reset_time - now_time
            if wait_sec < 0:
                wait_sec = 0
            restart_t = self.twitterapis[selected_account] \
                            .trans_time_obj_str(int(min_reset_time + 2),
                                                "unix",
                                                "mysql")
            msg = ("All account api limit expired."
                   " Wait for %s second. "
                   "Start at %s" % (wait_sec, restart_t))
            print(msg)

            time.sleep(wait_sec + 2)

            self.twitterapis[selected_account].updateClientStatus()

        return selected_account

    def selectKey(self):
        """
        keyStatusを元に検索するkeyとmodeを選択.

        Result:
            selected_key: 選択した検索key
            mode (str): 検索モード"new"/"paging"/"update"のいずれか
        """
        recent_mins = []
        since_tw_ids = []
        diff_tw_ids = []
        last_updated_times = []
        none_idx = None

        for i, k in enumerate(self.keys):
            recent_min = self.keystatuses[k]["recent_min"]
            since_tw_id = self.keystatuses[k]["since_tw_id"]
            last_update_time = self.keystatuses[k]["last_updated_time"]

            recent_mins.append(recent_min)
            since_tw_ids.append(since_tw_id)
            last_updated_times.append(last_update_time)

            # "update"モード移行前のクロール(Case1 ~ Case3)が済んでいないkeyを優先的に検索
            if (recent_min is None) or (since_tw_id is None) or \
                                       (last_update_time is None):
                if none_idx is None:
                    none_idx = i
                diff_tw_id = None
            # 全てのkeyがupdateモードに移行している場合はsince_idと最後のクロールの最小idの差を計算
            else:
                diff_tw_id = recent_min - since_tw_id
                # diff_tw_idはpagingが完了していない間は正の値を，完了したら０もしくは負になる
                # ０になるのは、updateしてもツイートが取得できなかった時

            diff_tw_ids.append(diff_tw_id)

        # "update"モード移行前のクロール
        if (None in recent_mins) or (None in since_tw_ids) or \
                                    (None in last_updated_times):
            selected_key = self.keys[none_idx]

            # Case1. 初回のクロール
            if recent_mins[none_idx] is None:
                mode = "new"
            # Case2. 2回目以降のクロール
            else:
                mode = "paging"

        # "update"モード移行後のクロール
        else:
            max_diff = max(diff_tw_ids)

            # Case3. paging中のkeyがある場合
            if max_diff > 0:
                idx = diff_tw_ids.index(max_diff)
                selected_key = self.keys[idx]
                mode = "paging"
            # Case4. paging中のkeyがない場合
            else:
                min_last_updated_time = min(last_updated_times)
                idx = last_updated_times.index(min_last_updated_time)
                selected_key = self.keys[idx]
                mode = "update"

        return selected_key, mode

    def set_keyStatus_to_acc(self, t_api, key):
        """
        与えたtwitterAPIクラスに，keyStatusの情報を与える.

        Args:
            t_api : TwitterAPIクラス
            key (str or int): 検索key
        """
        if self.search_type == "word":
            t_api.word = key
        else:
            t_api.user = key
        t_api.max_tw_id = self.keystatuses[key]["max_tw_id"]
        t_api.recent_min = self.keystatuses[key]["recent_min"]
        t_api.since_tw_id = self.keystatuses[key]["since_tw_id"]

    def crawl_once(self):
        """
        与えられた条件下で一回クロールする.

        Return:
            crawled_df : 取得したツイートのデータフレーム
        """
        account = self.selectClient()  # クロールするアカウントの選択
        twitter_account = self.twitterapis[account]

        selected_key, mode = self.selectKey()  # クロールするkeyの選択

        # TwitterAPIインスタンスにkeyやwordStatusをセット
        self.set_keyStatus_to_acc(twitter_account, selected_key)

        msg = ("search key: '%s', twitter account: '%s', mode: %s"
               % (selected_key, account, mode))
        print(msg)

        # 検索する
        if self.search_type == "word":
            twitter_account.search_type = "word"
            crawled_df = pd.DataFrame(twitter_account.search(mode,
                                                             verbose=False))
        else:
            twitter_account.search_type = "user"
            crawled_df = pd.DataFrame(twitter_account.search(mode,
                                                             verbose=False))

        self.updateKeyStatus(twitter_account, selected_key)

        crawled_time_msg = ("Crawled %s ~ %s, %s tweets." %
                            (twitter_account.crawled_min_t,
                             twitter_account.crawled_max_t,
                             twitter_account.crawled_num))
        print(crawled_time_msg)
        crawled_num_msg = ("Total crawled num: %s\n" %
                           self.keystatuses[selected_key]["total_crawled_num"])
        print(crawled_num_msg)

        return crawled_df

    def run(self, ask_runtime=True, export_lap=900, full_runtime=10800):
        """アカウントを切り替えつつクロールし続ける."""
        if ask_runtime:
            full_runtime = int(input("Enter Runtime (minutes): ")) * 60
        i = 0
        start_time = int(time.time())
        lap_start = int(time.time())
        file_num = 0
        first_flag = True

        while(True):

            i += 1
            print("####CRAWL NO: %s ####" % i)

            if first_flag:
                result_df = self.crawl_once()
                first_flag = False
            else:
                result_df = pd.concat([result_df, self.crawl_once()])

            laptime = int(time.time()) - lap_start
            runtime = int(time.time()) - start_time

            if laptime > export_lap:
                file_num += 1
                pickle_path = "./results/result_crawlNo%s.pkl" % file_num
                with open(pickle_path, "wb") as f:
                    pkl.dump(result_df, f)
                first_flag = True
                msg = ("\n######saved result to ./results/result_crawlNo%s.pkl"
                       "######\n" % file_num)
                print(msg)
                lap_start = int(time.time())

            if runtime > full_runtime:
                file_num += 1
                pickle_path = "./results/result_crawlNo%s.pkl" % file_num
                with open(pickle_path, "wb") as f:
                    pkl.dump(result_df, f)
                msg = ("\n######saved result to ./results/result_crawlNo%s.pkl"
                       "######\n" % file_num)
                print(msg)
                print("Finish Process.")
                break
        print(self.keystatuses)
        self.save_keystatus()
