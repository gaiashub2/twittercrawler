# Twitter Crawler

## 一言で言うと
複数のTwitterアカウントを使用して，アカウントのrate_limitを考慮しつつツイートをクロールするスクリプト．

## できること
* キーワード検索（キーワードごとに限界まで遡ったのち，更新する）
* ユーザタイムライン検索（ユーザごとに限界まで遡ったのち，更新する）
* 結果のcsv出力
* 結果のpickle出力（本文の改行文字も保持されるためおすすめ）
* リプライ・リツイートネットワークの作成（開発中）
* フォローネットワークの作成（開発中）

## Requirements
* Python 3.6.4

## Setup
* `accounts_sample.cfg`に倣って，アプリケーションとアカウントのkeyとsecretを記入した，`accounts.cfg`を作成する．
* キーワード検索の場合，`keywords_sample.csv`に倣って検索クエリを記入する（UTF-8で保存） : `keywords.csv`
* ユーザ検索の場合，`keyusers_sample.csv`に倣って検索ユーザのidまたはアカウント名を記入する : `keyusers.csv`

### Environment
Install using pip:

`pip install -r requirements.txt` 

### Account Information
クロールに使用するTwitterアプリケーションとユーザアカウントのkeyとsecretを取得する．
[こちらのサイト](https://syncer.jp/Web/API/Twitter/REST_API/)が参考になる．

### Keywords : KeyUsers
キーワード検索とユーザタイムライン検索に対応(UTF-8でエンコードすること)

## 実行
キーワード検索の場合は，`python keyword_search.py`
ユーザタイムライン検索の場合は，`python user_search.py`
Enter Runtime (minutes): に対し，プログラムを回す時間を記入する．

### 細かい機能
ツイートの取得状況は，crawl_metadata.pklに保存される．
プログラムを実行する際，このファイルを読み込むため重複する検索を行わずにすむ．
（したがって，一からクロールし直したい際はこのファイルを改名もしくは削除すること）

## 謝辞
[m-ochi](https://github.com/m-ochi)さんから頂いたコードを参考にさせていただきました．