import re
import pandas as pd
from sqlalchemy import create_engine


class DataPreprocessor:
    def __init__(self):
        self.engine = create_engine('mysql+pymysql://root:123456@localhost/'
                                    'gra_pro')
        self.query = """
                     SELECT *
                     FROM `comment`
                     """
        self.df = pd.read_sql(self.query, self.engine)
        self.df['comm_cont'] = self.df['comm_cont'].apply(
            lambda x: x.lower() if isinstance(x, str) else x)

    def clean_url(self):
        pattern = re.compile(r'<img[^>]+>', flags=re.IGNORECASE)
        self.df = self.df[
                ~self.df['comm_cont'].str.contains(pattern, na=False)
        ]
        return self.df

    def clean_escp_ch(self):
        pattern = r'[\n\t\r\v\0]'
        self.df['comm_cont'] = self.df['comm_cont'].apply(
            lambda x: re.sub(pattern, '', x))
        return self.df

    def process_en(self):
        curse = {'nb': '牛逼', 'nba': '美职篮', 'cba': '中职篮', 'goat': '世界最佳',
                 'sb': '傻逼', 'rip': '节哀', 'qnmd': '肏你妈', 'respect': '尊敬',
                 'cy': '插眼', 'gun': '滚', 'fw': '废物', 'qnmlgb': '肏你妈',
                 'g': '完蛋', 'nice': '漂亮', 'good': '漂亮', 'tm': '他妈的',
                 'tmd': '他妈的', 'md': '他妈的', 'w': '万', 'fifa': '国际足联',
                 'ai': '人工智能', 'vs': '对阵', 'mvp': '最佳球员', 'ok': '可以',
                 'xjbt': '乱踢', 'messi': '梅西', 'wtf': '搞什么呀', 'var': '视频助理',
                 'buff': '加成', 'factos': '事实', 'bb': '瞎嚷嚷'}

        def replace_en(match):
            word = match.group(0)
            return curse[word] if word in curse else word

        replacement_template = "{}岁以下"

        def replace_age(match):
            age = match.group(1)
            return replacement_template.format(age)

        def process_text(text):
            pattern_punctuations = r'[.]'
            text = re.sub(pattern_punctuations, '', text)
            pattern_age = r"u(\d+)"
            temp_text = re.sub(pattern_age, replace_age, text)

            pattern = re.compile(r'[a-zA-Z]+')
            replaced_text = re.sub(pattern, replace_en, temp_text)
            replaced_text = replaced_text.replace('oppo春节传好运', '')
            return replaced_text

        self.df['comm_cont'] = self.df['comm_cont'].apply(process_text)

        en_rows = self.df[self.df['comm_cont'].str.contains(r'[a-zA-Z]+')]

        names = ('c罗', 'b费', 'b席', 'big6')
        drop_df = en_rows[~en_rows['comm_cont'].str.contains('|'.join(names))]
        drop_df.to_excel('output.xlsx', index=False)

        return drop_df

    def count_char(self):
        english_df = self.df[self.df['comm_cont'].str.contains(r'[a-zA-Z]+')]
        pattern = r'[a-zA-Z\s]+'
        new_df = english_df['comm_cont'].apply(
            lambda row: ' '.join(re.findall(pattern, row)).strip())

        def split_values_with_spaces(series):
            new_values = []

            for value in series:
                if ' ' in value:
                    split_items = value.split(' ')
                    for item in split_items:
                        new_values.append(item.strip())
                else:
                    new_values.append(value)

            new_series = pd.Series(new_values)
            new_series = new_series.str.strip()

            return new_series

        new_df = split_values_with_spaces(new_df)
        word_counts = new_df.value_counts()

        return word_counts

    def write_info(self):
        self.df.to_sql('handled_comment', test.engine, if_exists='replace',
                       index=False)


test = DataPreprocessor()
df = test.df
df = test.clean_url()
df = test.clean_escp_ch()
drop_df = test.process_en()
drop_list = drop_df['comm_id'].to_list()
df = df[~df['comm_id'].isin(drop_list)]
print(df)
