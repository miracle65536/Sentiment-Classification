import requests
import re
import threading
import pymysql
import emoji
import concurrent.futures
import pandas as pd
from sqlalchemy import create_engine


class DataScraper:
    def __init__(self):
        self.df = pd.DataFrame(columns=[
            'art_id',
            'art_title',
            'art_tag',
            'art_pub_time'
        ])
        self.headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                    'Chrome/58.0.3029.110 '
                  }
        self.conn = pymysql.connect(
            host='localhost',
            user='root',
            password='123456',
            port=3306,
            db='gra_pro',
            charset='utf8mb4',
            autocommit=False
        )
        self.base_article_url = "https://api.dongqiudi.com/v3/archive/app" \
                                "/tabs/getlists?id=104&platform=android" \
                                "&child_tab_id=0&&action=fresh" \
                                "&version=349&from=tab_104"
        self.base_comm_url = 'https://api.dongqiudi.com/v2/article/' \
                             '{}/comment?sort=down'

    def create_table(self):
        cursor = self.conn.cursor()
        sql = "CREATE DATABASE IF NOT EXISTS gra_pro"
        cursor.execute(sql)
        cursor.execute('USE gra_pro')
        try:
            cursor.execute('DROP TABLE IF EXISTS comment')
            sql = '''
                CREATE TABLE comment (
                    comm_id INT PRIMARY KEY AUTO_INCREMENT NOT NULL,
                    art_id VARCHAR(10) NOT NULL,
                    art_title VARCHAR(50),
                    art_tag VARCHAR(20),
                    art_pub_time timestamp,
                    comm_crtd_time timestamp,
                    comm_up INT,
                    comm_reply INT,
                    comm_cont TEXT NOT NULL,
                    comm_sent VARCHAR(8)
                )
            '''
            cursor.execute(sql)
        except Exception as e:
            print('创建表失败:', e)
        cursor.close()
        self.conn.close()

    def clear_table(self):
        cursor = self.conn.cursor()
        truncate_table_query = "TRUNCATE TABLE comment"
        cursor.execute(truncate_table_query)
        self.conn.commit()
        cursor.close()
        self.conn.close()

    def contains_emoji(self, text):
        emoji_pattern = re.compile("["
                                   u"\U0001F600-\U0001F64F"
                                   u"\U0001F300-\U0001F5FF"
                                   u"\U0001F680-\U0001F6FF"
                                   u"\U0001F1E0-\U0001F1FF"
                                   u"\U00002702-\U000027B0"
                                   u"\U000024C2-\U0001F251"
                                   "]+", flags=re.UNICODE)
        return bool(emoji_pattern.search(text))

    def handle_emoji(self, str):
        trans_str = str
        if self.contains_emoji(str) is True:
            trans_str = emoji.demojize(str, language='zh')

        return trans_str

    def get_comments(self, article_id):
        df_comm = pd.DataFrame(columns=[
            'art_id',
            'comm_crtd_time',
            'comm_up',
            'comm_reply',
            'comm_cont',
            'comm_sent'
        ])
        url = self.base_comm_url.format(article_id)
        comm_cnt = 0
        while url:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                data = response.json()['data']
                next_url = data.get('next')
                comments = data['comment_list']
                for comment in comments:
                    up = comment['up']
                    reply = comment['reply_total']
                    crtd_time = comment['created_at']
                    content = comment['content']

                    row_data = {
                        'art_id': article_id,
                        'comm_crtd_time': crtd_time,
                        'comm_up': up,
                        'comm_reply': reply,
                        'comm_cont': content
                    }
                    df_comm = pd.concat([df_comm, pd.DataFrame([row_data])],
                                        ignore_index=True)

                    comm_cnt += 1
                url = next_url
            else:
                print('failure:{},url is {}'.format(response.status_code, url))
                break

        print("{}:Total number of comments:{}".format(article_id, comm_cnt))
        # print(df_comm)
        return df_comm

    def get_articles(self, url):
        page_num = 0
        article_num = 0
        tmp_response = requests.get(url, headers=self.headers)
        url = tmp_response.json()['fresh']
        while url:
            page_num += 1
            try:
                response = requests.get(url, headers=self.headers)
            except requests.exceptions.ConnectionError:
                print(url)
            if response.status_code == 200:
                contents = response.json()['contents']
                next_url = response.json()['next']
                if len(contents) > 0 and contents[0] is not None:
                    articles = contents[0]['articles']
                else:
                    break
                for article in articles:
                    id = article['id']
                    title = article['title']
                    pub_time = article['published_at']
                    topic_tags = article.get('topic_tags', [])
                    if topic_tags:
                        tag = topic_tags[0].get('content', '')
                    else:
                        tag = ''

                    row_data = {
                        'art_id': id,
                        'art_title': title,
                        'art_tag': tag,
                        'art_pub_time': pub_time
                    }
                    self.df = pd.concat([self.df, pd.DataFrame([row_data])],
                                        ignore_index=True)

                    article_num += 1
                url = next_url
            else:
                print('failure:{},url is {}'.format(response.status_code, url))
                break

        print("Total number of pages: {}\t"
              "Total number of articles: {}".format(page_num, article_num))

    def process_article(self, art_id):
        df_temp = self.get_comments(art_id)

        current_thread = threading.current_thread()
        print("当前线程：", current_thread.name)

        return df_temp

    def handle_article(self):
        df_comm = pd.DataFrame()
        lock = threading.Lock()
        max_threads = 16

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_threads
        ) as executor:
            tasks = []
            task_cnt = 0
            for art_id in self.df['art_id']:
                future = executor.submit(self.process_article, art_id)
                tasks.append(future)

            for future in concurrent.futures.as_completed(tasks):
                with lock:
                    df_comm = pd.concat([df_comm, future.result()],
                                        ignore_index=True)
                    print(df_comm)
                    task_cnt += 1

                thread_count = len(executor._threads)
                print(f"No.{task_cnt} 当前线程池中的线程数量: {thread_count}")

        self.df = pd.merge(self.df, df_comm, on='art_id', how='right')
        comment_num = len(self.df)
        print("共{}条数据".format(comment_num))
        self.df.drop_duplicates(['art_id', 'comm_cont'])
        self.df = self.df[self.df['comm_cont'] != '']
        self.df['comm_cont'] = self.df['comm_cont'].apply(
            lambda x: self.handle_emoji(x)
        )
        print(f"过滤完成，共{len(self.df)}条数据")

    def write_info(self):
        engine = create_engine('mysql+pymysql://root:123456@localhost/gra_pro')

        self.df.index.name = 'comm_id'

        self.df.to_sql(name='comment', con=engine, if_exists='replace',
                       index=True)


test = DataScraper()
test.get_articles(test.base_article_url)
test.handle_article()
test.clear_table()
test.write_info()
