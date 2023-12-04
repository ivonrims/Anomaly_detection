import numpy as np
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

import io
import telegram

import matplotlib.pyplot as plt

import seaborn as sns

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# parameters for the tg-bot
chat_id = -4095906857
my_token = '6384523160:AAHLGiud-LcmjE-COmJie_wcyXnxeuGw3r4'

# default arguments to be sent into tasks
default_args = {
    'owner': 'il-smirnov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2023, 11, 17),
}

# DAG launch interval (we make check up for abnormal values every 15 minutes)
schedule_interval = '*/15 * * * *'

def find_outliers(df, metric, n=12, alpha=4.5): # n - the number of previous periods for calculating quantiles, by default we will set it to 12 (3 hours - determined by trial and error)
    current_value = df[metric].iloc[0] # metric value for the previous 15 minutes
    previous_value = df[metric].iloc[1] # metric value for the current 15 minutes
    df['q_25'] = df[metric].iloc[::-1].shift(1).rolling(n).quantile(.25).copy() # 25% percentile for n previous periods
    df['q_75'] = df[metric].iloc[::-1].shift(1).rolling(n).quantile(.75).copy() # 75% percentile for n previous periods
    df['ICR'] = df.q_75 - df.q_25
    df['upper_value'] = df.q_75 + alpha*df.ICR
    df['lower_value'] = df.q_25 - alpha*df.ICR
    #df['upper_value'] = df['upper_value'].iloc[::-1].rolling(n, center=True).mean() # smooth the boundaries of the confidence intervals
    #df['lower_value'] = df['lower_value'].iloc[::-1].rolling(n, center=True).mean() 
    if current_value < df.lower_value.iloc[0] or current_value > df.upper_value.iloc[0]:
        is_alert = 1
        diff = 100*(current_value/previous_value - 1).round(2) # diff - deviation from the previous value in %
    else:
        is_alert = 0
        diff = 0
    return current_value, is_alert, diff, df

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def is_alert_bot():
    @task()
    def extract_data():
        # parameters for connecting to ClickHouse
        connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': 'simulator'
        }
        
        q = """select * from
            (select toStartOfFifteenMinutes(time) ts, uniqExact(user_id) DAU, 
            countIf(action='view') views, countIf(action='like') likes,
            countIf(action='like')/countIf(action='view') CTR
            from simulator_20231020.feed_actions
            where time >= today() - interval 3 hour and time < toStartOfFifteenMinutes(now()) 
            group by toStartOfFifteenMinutes(time)
            order by ts desc) t1

            join

            (select toStartOfFifteenMinutes(time) ts, count(user_id) messages_sent
            from simulator_20231020.message_actions
            where time >= today() - interval 3 hour and time < toStartOfFifteenMinutes(now()) 
            group by toStartOfFifteenMinutes(time)
            order by ts desc) t2

            using(ts)"""

        df = ph.read_clickhouse(q, connection=connection)
        return df
    
    @task()
    def send_alert(df):
        metrics = ['DAU', 'views', 'likes', 'CTR', 'messages_sent']
        for metric in metrics:
            current_value, is_alert, diff, df = find_outliers(df, metric)
            if is_alert:
                msg = f'Метрика {metric}: текущее значение = {current_value}, время = {df.ts.iloc[0]}, отклонение за последние 15 минут на {diff}%'
                fig, ax = plt.subplots()
                ax.plot(df.ts, df[metric], label=f'{metric}')
                ax.plot(df.ts, df.upper_value, label='Upper bound')
                ax.plot(df.ts, df.lower_value, label='Lower bound')
                ax.grid(axis='both', alpha=.3)
                ax.set_title(metric)
                ax.legend()       

                plot_alert = io.BytesIO() # create a file object
                plt.savefig(plot_alert)
                plot_alert.seek(0) # 
                plot_alert.name = 'alert.png'
                plt.close()

                bot = telegram.Bot(token=my_token) # получаем доступ
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_alert)
            
    df = extract_data()
    send_alert(df)
is_alert_bot = is_alert_bot()
    
    