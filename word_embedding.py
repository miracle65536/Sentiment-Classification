import re
import pandas as pd
import torch.utils
import torch.utils.data
from transformers import BertTokenizer
import torch

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")


class Dataset(torch.utils.data.Dataset):
    def __init__(self, df):
        self.dataset = df

    def __len__(self):
        return len(self.dataset)

    def __getitem__(self, index):
        text = self.dataset[index]['comm_cont']
        label = self.dataset[index]['comm_sent']

        return text, label


class BertPreTrained:
    def __init__(self):
        self.BERT_PATH = './bert-base-chinese'
        self.tokenizer = BertTokenizer.from_pretrained(self.BERT_PATH)
        self.new_tokens = ['c罗', 'b费', 'b席', 'big6']
        self.num_added_toks = self.tokenizer.add_tokens(self.new_tokens)

    def collate_fn(self, data):
        cont = [i[0] for i in data]
        sent = [i[1] for i in data]

        bert_input = self.tokenizer(cont,
                                    padding='max_length',
                                    max_length=256,
                                    truncation=True,
                                    return_tensors="pt"
                                    )
        input_ids = bert_input['input_ids']
        token_type_ids = bert_input['token_type_ids']
        attention_mask = bert_input['attention_mask']
        labels = torch.LongTensor(sent)

        return input_ids, attention_mask, token_type_ids, labels


class Word_Embedding:
    def __init__(self):
        self.df = pd.read_excel('comments.xlsx')

    def clean_punc(self):
        def remove_punctuation(text):
            pattern = r'[\W_\u2460-\u2469⒈-⒏\u00BD₀-₉⑴-⑸²-⁹]'
            cleaned_text = re.sub(pattern, '', text)
            return cleaned_text

        self.df['comm_cont'] = self.df['comm_cont'].astype(str)
        self.df['comm_cont'] = self.df['comm_cont'].apply(remove_punctuation)
        self.df = self.df[self.df['comm_cont'].notnull()]
        self.df = self.df[self.df['comm_cont'].str.strip() != '']
        return self.df


test = Word_Embedding()
df = test.clean_punc()
