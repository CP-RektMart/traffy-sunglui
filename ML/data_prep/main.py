import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
import requests
from ML.utils.logger import log_decorator

@log_decorator
def load_df(path):
    return pd.read_csv(path)

@log_decorator
def calculate_duration(df):
    def toDate(serie):
        return pd.to_datetime(serie, format='ISO8601').dt.tz_localize(None) + timedelta(hours=7)
    
    df = df.copy()
    df = df[df['state'] == 'เสร็จสิ้น']

    df['timestamp'] = toDate(df['timestamp'])
    df['last_activity'] = toDate(df['last_activity'])
    df['duration'] = (df['last_activity'] - df['timestamp']).dt.total_seconds() // 60
    
    return df

@log_decorator
def handleNull(df: pd.DataFrame):
    df['type'].fillna('{}')
    df['organization'].fillna('')
    df.dropna(axis=0, how='any', subset=['timestamp', 'last_activity'], inplace=True)
    return df

@log_decorator
def encode_types(df):
    target_types = {
        '',
        'ป้าย',
        'ความสะอาด',
        'แสงสว่าง',
        'สอบถาม',
        'ร้องเรียน',
        'การเดินทาง',
        'จราจร',
        'ท่อระบายน้ำ',
        'สะพาน',
        'เสียงรบกวน',
        'ต้นไม้',
        'คนจรจัด',
        'คลอง',
        'ถนน',
        'เสนอแนะ',
        'กีดขวาง',
        'สายไฟ',
        'PM2.5',
        'น้ำท่วม',
        'ทางเท้า',
        'สัตว์จรจัด',
        'ความปลอดภัย',
        'ห้องน้ำ',
        'ป้ายจราจร'
    }

    def parse(s):
        try:
            return s.strip('{}').split(',')
        except:
            return []
    
    df['type_list'] = df['type'].apply(parse)

    for cat in target_types:
        df[cat] = df['type_list'].apply(lambda lst: int(cat in lst))

    df['Others'] = df['type_list'].apply(
        lambda lst: int(any(c not in target_types for c in lst))
    )

    df.drop(columns='type_list', inplace=True)
    
    return df

@log_decorator
def calculate_target(df):
    start_time = time(9, 30)
    end_time = time(15, 30)

    def minutes_to_next_working_hour(dt):
        weekday = dt.weekday()  # 0 = Monday, ..., 6 = Sunday
        current_time = dt.time()

        if weekday >= 5 or current_time >= end_time:
            days_ahead = 1
            while (dt + timedelta(days=days_ahead)).weekday() >= 5:
                days_ahead += 1
            next_working_start = datetime.combine((dt + timedelta(days=days_ahead)).date(), start_time)
            return int((next_working_start - dt).total_seconds() // 60)

        elif current_time < start_time:
            today_start = datetime.combine(dt.date(), start_time)
            return int((today_start - dt).total_seconds() // 60)

        else:
            return 0

    df['until_working_time'] = df['timestamp'].apply(minutes_to_next_working_hour)
    
    return df

def orgs_wrapper(path, url):
    @log_decorator
    def orgs(df):
        resp = requests.get(url)
        data = resp.json()
        orgs = data['results']
        pd.DataFrame(orgs).to_csv(path, index=False)

        orgs = pd.read_csv(path)
        orgs.set_index('fonduegroup_name', inplace=True)
        
        # Define target columns
        target_cols = ['avg_star', 'post_finish_percentage', 'avg_duration_minutes_finished']

        # Convert organization string to list
        df['orgs_list'] = df['organization'].str.strip().str.split(',')

        # Create df with NaNs in target columns
        df[target_cols] = np.nan

        # Explode orgs_list to merge with orgs
        exploded = df[['orgs_list']].explode('orgs_list').reset_index()
        exploded['orgs_list'] = exploded['orgs_list'].str.strip()

        # Join with orgs DataFrame
        merged = exploded.merge(orgs[target_cols], left_on='orgs_list', right_index=True, how='left')

        # Aggregate mean values for each original row
        means = merged.groupby('index')[target_cols].mean()

        # Assign the means back to df
        df.loc[means.index, target_cols] = means

        # Result
        df[['orgs_list'] + target_cols]

        df.drop(columns=['orgs_list'], inplace=True)
        
        return df
    
    return orgs

@log_decorator
def normalize(df):
    df = df.copy()

    df = df[df['duration'] > 0]
    df['log_duration'] = df['duration'].apply(np.log1p) 
    
    lower_bound = 6
    upper_bound = 20

    log_duration = df['log_duration']
    df = df[(log_duration >= lower_bound) & (log_duration <= upper_bound)]

    return df

@log_decorator
def select_cols(df):
    types = [
        'ป้าย',
        'ความสะอาด',
        'แสงสว่าง',
        'สอบถาม',
        'ร้องเรียน',
        'การเดินทาง',
        'จราจร',
        'ท่อระบายน้ำ',
        'สะพาน',
        'เสียงรบกวน',
        'ต้นไม้',
        'คนจรจัด',
        'คลอง',
        'ถนน',
        'เสนอแนะ',
        'กีดขวาง',
        'สายไฟ',
        'PM2.5',
        'น้ำท่วม',
        'ทางเท้า',
        'สัตว์จรจัด',
        'ความปลอดภัย',
        'ห้องน้ำ',
        'ป้ายจราจร',
        'Others'
    ]

    feature_cols = [
        'until_working_time',
        'avg_star',
        'post_finish_percentage',
        'avg_duration_minutes_finished',
    ]

    target_cols = [
        'duration',
        'log_duration',
    ]

    cols = target_cols + feature_cols + types
    
    return df[cols]

@log_decorator
def impute(df):
    na_cols = ['avg_star', 'post_finish_percentage', 'avg_duration_minutes_finished']

    df[na_cols] = df[na_cols].fillna(df[na_cols].mean())

    return df

@log_decorator
def save(df, path):
    df.to_csv(path, index=False)

load_path = "../data/bangkok_traffy.csv"
org_path = '../data/org.csv'
org_url = f'https://publicapi.traffy.in.th/premium-org-fondue/statistic-rank/top-rank-avg-star?org_key=bangkok&limit=100'
save_path = '../data/clean2.csv'

pipeline = [
    handleNull,
    calculate_duration,
    encode_types,
    calculate_target,
    orgs_wrapper(org_path, org_url),
    normalize,
    select_cols,
    impute
]

if __name__ == "__main__":
    df = load_df(load_path)

    for step in pipeline:
        df = step(df)

    save(df, save_path)