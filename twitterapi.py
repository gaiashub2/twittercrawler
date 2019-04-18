"""
corresponds with one twitter account.

# -*- coding: utf-8 -*-
Created on Jan 21, 2019
Updated on Feb 2, 2019

@author: g-suzuki
"""

import json
import datetime
import time
import csv
import os


class SampleError:
    """
    デバッグ用の仮想エラー.

    Attributes:
        status_code (str or int): virtual response code
        text (str): json object that contains response info
    """

    def __init__(self, status_code, error_message="エラーが起こってます！！"):
        """クラスコンストラクタ."""
        self.status_code = str(status_code)
        virtual_message_dic = {"errors": [{"message": error_message}]}
        self.text = json.dumps(virtual_message_dic)


class TwitterAPI:
    """
    APIを叩くクラス. 検索・例外処理・API制限対策・取得データの処理の機能あり.

    現時点ではキーワード検索とユーザ検索に対応.

    Attributes
    ----------
    url1 : str
        URL for searching user timelines.
    url2 : str
        URL for searching by keywords.
    url3 : str
        URL for searching one tweet by id.
    url4 : str
        URL for checking rate limit.
    name : str
        name of the instance
    twitter : requests_oauthlib.oauth1_session.OAuth1Session
        twitter instance created by oauth.
    search_lang : str
        search language based on ISO 639-1. Default: 'ja'
    search_type : str
        "word" if word based search, "user" if user based search.
    clientStatus : dict
        dict that contains info about rate limits.
    word : str
        search keyword. (on word based search)
    user : str or int
        search user. (on user based search). screen name(str) or user id(int)
    max_tw_id : int
        the max id num among all tweets crawled previously about the same key.
    recent_min : int
        the min id num among tweets crawled recently about the same key
    since_tw_id : int
        would not crawl tweets with id smaller than this.
    updated_time : int
        the last time this account crawled tweets.
    crawled_num : int
        the number of tweets crawled this time.
    crawled_max : int
        the max id num among tweets crawled this time.
    crawled_max_t : str
        the posted time of the newest tweet crawled this time.
    crawled_min : int
        the min id num among tweets crawled this time.
    crawled_min_t : str
        the posted time of the oldest tweet crawled this time.
    saving_dir : str
        the name of folder to export the results
    saving_filename : str
        the name of file to export the result.
    write_to_csv : bool
        if true, export the results to a csv file.
    """

    def __init__(self, account_name, twitter, lang="ja",
                 search_type="word", word=None, user=None,
                 since_tw_id=None, saving_dir="./results/",
                 saving_filename=None, write_to_csv=True):
        """クラスコンストラクタ."""
        self.url1 = "https://api.twitter.com/1.1/statuses/user_timeline.json"
        self.url2 = "https://api.twitter.com/1.1/search/tweets.json"
        self.url3 = "https://api.twitter.com/1.1/statuses/show.json"
        self.url4 = ("https://api.twitter.com/1.1/application/"
                     "rate_limit_status.json")
        self.name = account_name
        self.twitter = twitter
        self.search_lang = lang
        self.search_type = search_type
        self.clientStatus = {"word": {}, "user": {}}
        self.updateClientStatus()  # dict of reset_time and remaining
        self.word = word
        self.user = user
        self.max_tw_id = None  # newest tweet id so far
        self.recent_min = None  # oldest tweet id of tweets recently
        self.since_tw_id = since_tw_id  # wouldn't crawl tweets older than this
        self.updated_time = None  # last time crawled
        self.crawled_num = 0  # number of tweets crawled
        self.crawled_max = None  # newest tweet id crawled this time
        self.crawled_max_t = None  # newest posted time crawled this time
        self.crawled_min = None  # oldest tweet id crawled this time
        self.crawled_min_t = None  # oldest posted time crawled this time
        self.saving_dir = saving_dir
        self.saving_filename = saving_filename
        self.write_to_csv = write_to_csv

    def get_virtual_res(self, status_code, error_message="エラーが起こってます！！"):
        """仮想エラーを返す."""
        virtual_res = SampleError(status_code, error_message)
        return virtual_res

    def check_api_limit(self):
        """
        API制限を確認する.

        Return: 
            ret_dic (dict): APIを叩いた結果
        """
        ret = self.get_virtual_res("check_api_limit(), 1st loop")

        while(str(ret.status_code) != "200"):
            try:
                ret = self.twitter.get(self.url4,
                                       params={"resources": ("account,"
                                                             "application,"
                                                             "blocks,"
                                                             "direct_messages,"
                                                             "followers,"
                                                             "friends,"
                                                             "friendships,"
                                                             "geo,help,lists,"
                                                             "saved_searches,"
                                                             "search,statuses,"
                                                             "trends,users")})
            except Exception as e:
                print('=== エラー発生 ===')
                print('type: ', str(type(e)))
                print('args: ', str(e.args))
                print('message: ', e.message)
                print('e自身：', str(e))
                ret = self.get_virtual_res("rate_limit api の呼び出し時のエラー")

            if str(ret.status_code) != "200":
                print("Client Value Exception !!: ", str(ret.status_code))
                print("sleep 10 sec")
                time.sleep(10)

        ret_dic = json.loads(ret.text)
        return ret_dic

    def get_search_api_rate_remaining(self):
        """
        アカウントの、残りの検索可能回数と、それがリセットされるまでの時間を取得.

        Return:
            w_remaining (int): キーワード検索の残機
            w_reset_time (int): キーワード検索の復活時刻
            u_remaining (int): ユーザ検索の残機
            u_reset_time (int): ユーザ検索の残機
        """
        res_dic = self.check_api_limit()

        w_remaining = (res_dic["resources"]
                              ["search"]
                              ["/search/tweets"]
                              ["remaining"])
        w_reset_time = (res_dic["resources"]
                               ["search"]
                               ["/search/tweets"]
                               ["reset"])
        u_remaining = (res_dic["resources"]
                              ["statuses"]
                              ["/statuses/user_timeline"]
                              ["remaining"])
        u_reset_time = (res_dic["resources"]
                               ["statuses"]
                               ["/statuses/user_timeline"]
                               ["reset"])
        return w_remaining, w_reset_time, u_remaining, u_reset_time

    def updateClientStatus(self, ret=None):
        """
        clientStausを更新.

        検索後の場合はrate_limit APIを叩かずレスポンスヘッダの"x-rate-limit-"の値を取得.

        Args:
            ret: APIを叩いたレスポンス
        """
        if ret is None:
            w_rem, w_res, u_rem, u_res = self.get_search_api_rate_remaining()
            self.clientStatus["word"]["remaining_count"] = w_rem
            self.clientStatus["word"]["reset_time"] = w_res
            self.clientStatus["user"]["remaining_count"] = u_rem
            self.clientStatus["user"]["reset_time"] = u_res
        else:
            rem = int(ret.headers["x-rate-limit-remaining"])
            res = int(ret.headers["x-rate-limit-reset"])
            self.clientStatus[self.search_type]["remaining_count"] = rem
            self.clientStatus[self.search_type]["reset_time"] = res
        return

    def make_params(self, mode, key, count=None):
        """
        APIに送るリクエストのパラメータを作成する.

        Args:
            mode (str): 検索モード: 'new'/'paging'/'update'
            key (str or int): 検索するキーワード/ユーザ
            count (int): 検索数

        Return:
            param_dict (dict): APIに送るリクエストのパラメータ
        """
        # キーワード検索
        if self.search_type == "word":

            # クエリベース検索の限界は100件
            if count is None:
                count = 100

            param_dict = {"q": key,
                          "lang": self.search_lang,
                          "result_type": "recent",
                          "count": count}

        # ユーザ検索
        elif self.search_type == "user":

            # ユーザ検索の限界は200件
            if count is None:
                count = 200

            param_dict = {"count": count}

            # user_id, screen_nameの双方に対応
            if type(key) == int:
                param_dict["user_id"] = key
            else:
                param_dict["screen_name"] = key

        # 新規検索
        if mode == "new":
            return param_dict
        # スクロール検索
        elif mode == "paging":
            # 前ページが存在しない場合、新規検索
            if self.recent_min is None:
                return param_dict
            else:
                param_dict["max_id"] = int(self.recent_min) - 1
            # 限界まで（since_idを定義することなく）遡ってスクロール検索
            if self.since_tw_id is None:
                return param_dict
            # since_idより過去は遡らない
            else:
                param_dict["since_id"] = int(self.since_tw_id) - 1
                return param_dict
        # 更新ツイート取得
        elif mode == "update":
            # 過去のページが存在しない場合、新規検索
            if self.max_tw_id is None:
                return param_dict
            # 過去のページのmax_idより過去のツイートは取得する必要なし
            else:
                param_dict["since_id"] = int(self.max_tw_id)
                return param_dict

    def trans_time_obj_str(self, value, input_type, output_type):
        """
        時間の表示形式の変換(Twitter APIからの時刻は標準時のため，9時間進める必要がある等).

        Args:
            value: 元の入力
            input_type: 入力のタイプ('tw_time'/'unix')
            output_type: 出力のタイプ('dt'/'mysql'/'YMD'/'YM')

        Return:
            output_type次第で様々
        """
        if input_type == "tw_time":
            st = time.strptime(value, '%a %b %d %H:%M:%S +0000 %Y')
            dt = datetime.datetime(st.tm_year,
                                   st.tm_mon,
                                   st.tm_mday,
                                   st.tm_hour,
                                   st.tm_min,
                                   st.tm_sec) + datetime.timedelta(hours=9)
        elif input_type == "unix":
            dt = datetime.datetime.fromtimestamp(int(value))

        if output_type == "dt":
            return dt
        elif output_type == "mysql":
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        elif output_type == "YMD":
            return dt.strftime("%Y%m%d")
        elif output_type == "YM":
            return dt.strftime("%Y%m")

    def get_and_set_attr(self, gdict, sdict, gkey, skey):
        """
        sdictにkeyが存在したら、gdictに入れる.

        Args:
            gdict (dict): 入力dict
            sdict (dict): 出力dict
            gkey (str): 入力dictのkey
            skey (str): 出力dictのkey
        """
        if gkey in list(gdict.keys()):
            sdict[skey] = gdict[gkey]
        else:
            sdict[skey] = None

    def strip_status(self, status):
        """
        取得データのうちいらないデータを削ぎ落とす.

        Args:
            status (dict): 元のstatus1ツイート分

        Return:
            result (dict): 加工されたstatus1ツイート分
        """
        result = {}
        for attr in ["id", "screen_name", "name", "created_at",
                     "followers_count", "friends_count", "favourites_count",
                     "statuses_count", "description", "profile_banner_url",
                     "profile_image_url"]:
            attr_name = "user_" + attr
            self.get_and_set_attr(status["user"], result, attr, attr_name)
        for attr in ["id", "in_reply_to_status_id_str",
                     "in_reply_to_user_id_str", "text",
                     "retweet_count", "favorite_count", "source"]:
            self.get_and_set_attr(status, result, attr, attr)
        result["time"] = self.trans_time_obj_str(status["created_at"],
                                                 "tw_time", "dt")
        cr_dt = self.trans_time_obj_str(result["user_created_at"],
                                        "tw_time",
                                        "dt")
        result["user_created_at"] = cr_dt

        return result

    def process_content(self, content, key):
        """
        APIで取得したJSONを使いやすいように加工し、辞書型にして返す.

        Args:
            content (dict or list): レスポンス.キーワード検索ではdict, ユーザ検索ではlist.
            key (str or int): 検索するキーワード/ユーザ

        Return:
            tw_ids (list): 取得ツイートのidリスト
            all_tweets (list): 取得ツイート情報
            crawled_max (int): 取得した最新ツイートのid
            crawled_max_t (str): 取得した最新ツイートの投稿時間
            crawled_min (int): 取得した最古のツイートのid
            crawled_min_t (int): 取得した最古のツイートの投稿時間
            crawled_num (int): 取得ツイート数
        """
        if self.search_type == "word":
            statuses = content["statuses"]
        elif self.search_type == "user":
            statuses = content
        tw_ids = []
        all_tweets = []

        for i, status in enumerate(statuses):
            a_tw = self.strip_status(status)
            a_tw["key"] = key
            all_tweets.append(a_tw)
            tw_ids.append(int(a_tw["id"]))

        if len(tw_ids):
            crawled_min = min(tw_ids)
            crawled_max = max(tw_ids)
            crawled_num = len(tw_ids)
            min_tw_time = statuses[tw_ids.index(crawled_min)]["created_at"]
            max_tw_time = statuses[tw_ids.index(crawled_max)]["created_at"]
            crawled_min_t = self.trans_time_obj_str(min_tw_time, "tw_time",
                                                                 "mysql")
            crawled_max_t = self.trans_time_obj_str(max_tw_time, "tw_time",
                                                                 "mysql")
        else:
            crawled_min = None
            crawled_min_t = None
            crawled_max = None
            crawled_max_t = None
            crawled_num = 0

        return tw_ids, all_tweets, crawled_max, crawled_max_t, \
            crawled_min, crawled_min_t, crawled_num

    def write_tweet_to_csv(self, all_tweets, key, file_type="date"):
        """
        ツイートをcsvに出力する.

        その際本文や自己紹介文の改行文字は取り除かれる.
        特別な指定がない場合，出力ファイル名はクエリ検索の場合はYYYYMMDD.csv，ユーザ検索の場合はYYYYMM.csvとなる

        Args:
            all_tweets (dict): process_contentにより加工されたツイート群
            key(str or int): 検索するキーワード/ユーザ
        """
        if not os.path.exists(self.saving_dir):
            os.makedirs(self.saving_dir)

        for a_tw in all_tweets:
            tweet = a_tw["text"].replace("\r\n", "")
            tweet = tweet.replace("\n", "")
            description = a_tw["user_description"].replace("\r\n", "")
            description = description.replace("\n", "")

            if self.saving_filename is None:

                if file_type == "date":
                    if self.search_type == "word":
                        fname = a_tw["time"].strftime("%Y%m%d")
                    elif self.search_type == "user":
                        fname = a_tw["time"].strftime("%Y%m%d")[0:6]

                elif file_type == "key":
                    fname = key

                save_filename = self.saving_dir + fname + ".csv"

            else:
                save_filename = self.saving_dir + self.saving_filename + ".csv"

            a_tw["time"] = a_tw["time"].strftime("%Y-%m-%d %H:%M:%S")
            a_tw["user_created_at"] = (a_tw["user_created_at"]
                                       .strftime("%Y-%m-%d %H:%M:%S"))

            a_tw_data = [key, a_tw["id"], a_tw["time"], a_tw["user_id"],
                         a_tw["user_screen_name"], a_tw["user_name"],
                         a_tw["user_created_at"], a_tw["user_followers_count"],
                         a_tw["user_friends_count"],
                         a_tw["user_favourites_count"],
                         a_tw["user_statuses_count"],
                         description, a_tw["user_profile_banner_url"],
                         a_tw["user_profile_image_url"],
                         a_tw["in_reply_to_status_id_str"],
                         a_tw["in_reply_to_user_id_str"],
                         tweet, a_tw["retweet_count"], a_tw["favorite_count"],
                         a_tw["source"]]

            write_header = False
            if not os.path.exists(save_filename):
                write_header = True
                header = ["key", "id", "time", "user_id",
                          "user_screen_name", "user_name", "user_created_at",
                          "user_followers_count", "user_friends_count",
                          "user_favourites_count", "user_statuses_count",
                          "user_description", "user_profile_banner_url",
                          "user_profile_image_url", "in_reply_to_status_id_str",
                          "in_reply_to_user_id_str", "tweet_text",
                          "retweet_count", "favorite_count", "source"]

            with open(save_filename, "a") as f:
                writer = csv.writer(f,
                                    delimiter=",",
                                    quotechar='"',
                                    lineterminator="\n",
                                    quoting=csv.QUOTE_ALL)
                if write_header:
                    writer.writerow(header)
                writer.writerow(a_tw_data)

        return

    def search(self, mode, key=None, count=None, verbose=True):
        """
        クエリに基づいてツイートを検索する.

        Args:
            mode (str): 検索モード('new'/'paging'/'update')
            key (str or int): 検索するキーワード/ユーザ
            count (int): 検索数
            verbose (bool): 途中経過をprintするか否か

        Return:
            all_tweets (list): 取得ツイート情報
            APIを叩けなかった場合はNone
        """
        if self.search_type == "word":

            if key is None:
                if self.word is None:
                    print("key word undefined")
                    return
                key = self.word

            url = self.url2
            error_type = "/search/tweets api の呼び出し時のエラー"

        elif self.search_type == "user":

            if key is None:
                if self.user is None:
                    print("key user undefined")
                    return
                key = self.user

            url = self.url1
            error_type = "/statuses/user_timeline api の呼び出し時のエラー"

        param_dict = self.make_params(mode, key, count)

        try:
            ret = self.twitter.get(url, params=param_dict)
            content = json.loads(ret.text)
        except Exception as e:
            print('=== エラー発生 ===')
            print('type: ', str(type(e)))
            print('args: ', str(e.args))
            ret = self.get_virtual_res(error_type)

        if str(ret.status_code) == "200":

            (tw_ids, all_tweets, crawled_max,
             crawled_max_t, crawled_min, crawled_min_t,
             crawled_num) = self.process_content(content, key)

            if (crawled_min == self.since_tw_id) and \
               (self.since_tw_id is not None):

                all_tweets.pop(tw_ids.index(crawled_min))

            if self.write_to_csv:
                self.write_tweet_to_csv(all_tweets, key)

            self.updated_time = int(time.time())
            self.crawled_num = crawled_num
            self.crawled_max = crawled_max
            self.crawled_max_t = crawled_max_t
            self.crawled_min = crawled_min
            self.crawled_min_t = crawled_min_t

            self.updateClientStatus(ret)

            if self.clientStatus[self.search_type]["remaining_count"] > 0:
                if verbose:
                    acc_msg = ("Account Name : %s , Search mode : %s"
                               % (self.name, mode))
                    crawl_msg = ("Crawled %s tweets from %s ~ %s"
                                 % (self.crawled_num, self.crawled_min,
                                    self.crawled_max))
                    time_msg = ("Time is %s ~ %s" % (self.crawled_min_t,
                                                     self.crawled_max_t))
                    remain = (self.clientStatus[self.search_type]
                                               ["remaining_count"])
                    remain_msg = ("remaining count is %s\n" % remain)
                    print(acc_msg)
                    print(crawl_msg)
                    print(time_msg)
                    print(remain_msg)

            else:
                now_time = int(time.time()) + 1
                wait_sec = (self.clientStatus[self.search_type]["reset_time"] -
                            now_time)
                if wait_sec < 0:
                    wait_sec = 0

                restart = self.clientStatus[self.search_type]["reset_time"] + 2
                reset_datetime = self.trans_time_obj_str(restart,
                                                         "unix",
                                                         "mysql")
                expire_msg = ("Account Name %s: api limit expired, wait %s "
                              "seconds. Start at %s"
                              % (self.name, wait_sec, reset_datetime))
                print(expire_msg)

                time.sleep(wait_sec + 2)
            return all_tweets

        else:
            print("Client Value Exception !!: ", str(ret.status_code))
            print("sleep 10 sec")
            time.sleep(10)

            return None
