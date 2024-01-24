# Автоматизированный отчет основных метрик с графиками, 
# в сравнении с предыдущим днем и отправкой в Telegram


import pandas as pd
import telegram 
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse
import io
from datetime import datetime, timedelta, date
from matplotlib.backends.backend_pdf import PdfPages
from telegram import InputFile
from airflow import DAG
from airflow.decorators import dag, task

# параметры для DAG 
default_args = {
    'owner': 'n-abramkin', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries' : 2, # Кол-во попыток выполнить DAG
    'retry_delay' : timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2023, 10, 16, 0, 0) # Дата начала выполнения DAG
}

schedule_interval = '30 10 * * *'

# создание бота
my_token = '****'
bot = telegram.Bot(token=my_token)
chat_id = ***

# инфо о последнем пользователе
# info = bot.getUpdates()
# print(info[-1])

# необходимы даты 
current_date = date.today()
day_ago = current_date - timedelta(days=1)
two_days_ago = current_date - timedelta(days=2)
seven_days_ago = current_date - timedelta(days=7)

# вычисление процента отлонения метрики (вчерашняго дня от позавчера)
def percent(df, column):
    begin_value = float(df[column][0])
    end_value =  float(df[column][1])
    percent = round(((begin_value - end_value) / end_value) * 100, 2)
    out =  f'+{percent} %' if percent > 0 else f'{percent} %'
    return str(out)

# вычисление разницы метрики (вчерашняго дня от позавчера)
def diff(df, column):
    difference = round(float(df[column][0]) - float(df[column][1]), 3)
    out =  f'+{difference}' if difference > 0 else f'{difference}'
    return str(out) 

# запрос в базу данных
def ch_get_df (query):
    connection = {
            'host': 'https://clickhouse...',
            'password': '*****',
            'user': '*****',
            'database': '*****'}
    
    result = pandahouse.read_clickhouse(query, connection=connection)
    return result

# отправка сообщения            
def message(text):
    return bot.sendMessage(chat_id=chat_id, text=text, parse_mode='Markdown')

# создание графика
def graph(df, x, y, title, pdf_pages):
    # размер графика и разрешение
    plt.figure(figsize=(12, 4), dpi=150)
    # график линии
    sns.lineplot(data=df, x=x, y=y)
    # добавление точек на график
    sns.scatterplot(data=df, x=x, y=y, color='blue', s=20)
    # добавление чисел на график
    for i in range(len(df[x])):
        plt.text(df[x][i], df[y][i], str(df[y][i]), ha='center', va='bottom')
    # начало графика с 0
    plt.ylim(0, max(df[y])+(max(df[y])*0.25))
    # заголовок
    plt.title(title);
    pdf_pages.savefig(plt.gcf())
    return


@dag(default_args=default_args, schedule_interval=schedule_interval)
def n_abramkin_telegram_report():
    
    @task()
    def report_feed():
        # REPORT FOR FEED
        
        # запрос в базу данных
        query = """
            SELECT toDate(time) AS date,
            COUNT (DISTINCT user_id) AS DAU,
            COUNTIf(user_id, action = 'view') AS count_views,
            COUNTIf(user_id, action = 'like') AS count_likes,
            ROUND(count_likes / count_views, 3) AS CTR,
            new_users.count_new_users AS new_users,
            new_users.count_organic_users AS organic_users,
            new_users.count_ads_users AS ads_users,
            new_posts.count_new_posts AS new_posts
            FROM feed_actions AS fa
            JOIN 
            -- количество новых пользователей, органических, по рекламе
            (SELECT date, COUNT(DISTINCT user_id) AS count_new_users,
            COUNTIf(DISTINCT user_id, source = 'organic') AS count_organic_users,
            COUNTIf(DISTINCT user_id, source = 'ads') AS count_ads_users
            FROM (SELECT user_id, MIN(toDate(time)) AS date, source
                  FROM feed_actions
                  GROUP BY user_id, source) 
            WHERE date BETWEEN (today() - 7) AND (today() - 1)
            GROUP BY date) AS new_users
            ON toDate(fa.time) = new_users.date
            JOIN 
            -- количество новых постов
            (SELECT date, COUNT(DISTINCT post_id) AS count_new_posts
            FROM (SELECT post_id, MIN(toDate(time)) AS date 
                  FROM feed_actions
                  GROUP BY post_id)
            WHERE date BETWEEN (today() - 7) AND (today() - 1)
            GROUP BY date) AS new_posts
            ON toDate(fa.time) = new_posts.date

            WHERE date BETWEEN (today() - 7) AND (today() - 1)
            GROUP BY date, new_users, organic_users, ads_users, new_posts
            ORDER BY date DESC
            """
        df = ch_get_df(query)
        
        # формирование текстового сообщения
        msg = f'''*1. Лента новостей на {day_ago} (отклонение от {two_days_ago}):*
        
*DAU:* {str(df['DAU'][0])} ({diff(df, 'DAU')} | {percent(df, 'DAU')});
*Количество просмотров:* 
{str(df['count_views'][0])} ({diff(df, 'count_views')} | {percent(df, 'count_views')});
*Количество лайков:* 
{str(df['count_likes'][0])} ({diff(df, 'count_likes')} | {percent(df, 'count_likes')});
*CTR:* {str(df['CTR'][0])} ({diff(df, 'CTR')} | {percent(df, 'CTR')});
*Количество новых постов:* 
{str(df['new_posts'][0])} ({diff(df, 'new_posts')} | {percent(df, 'new_posts')});
*Количество новых пользователей:* 
{str(df['new_users'][0])} ({diff(df, 'new_users')} | {percent(df, 'new_users')}), *в том числе:*
*- органических:* {str(df['organic_users'][0])} ({diff(df, 'organic_users')} | {percent(df, 'organic_users')});
*- по рекламе:* {str(df['ads_users'][0])} ({diff(df, 'ads_users')} | {percent(df, 'ads_users')}).

*Графики ленты новостей с {seven_days_ago} по {day_ago}:*
        '''
        # отправка сообщения
        message(msg)
        
        # создание PdfPages объекта
        pdf_pages_feeds = PdfPages(f'{seven_days_ago} - {day_ago}.pdf')
        
        # формирование графиков
        graph(df, 'date', 'DAU', 'DAU', pdf_pages_feeds)
        graph(df, 'date', 'count_views', 'Количество просмотров', pdf_pages_feeds)
        graph(df, 'date', 'count_likes', 'Количество лайков', pdf_pages_feeds)
        graph(df, 'date', 'CTR', 'CTR', pdf_pages_feeds)
        graph(df, 'date', 'new_users', 'Количество новых пользователей', pdf_pages_feeds)
        graph(df, 'date', 'organic_users', 'Количество новых органических пользователей', pdf_pages_feeds)
        graph(df, 'date', 'ads_users', 'Количество новых пользователей по рекламе', pdf_pages_feeds)
        graph(df, 'date', 'new_posts', 'Количество новых постов', pdf_pages_feeds)
        
        # закрываем PDF файл
        pdf_pages_feeds.close()
        
        # открытие и отправка файла (графиков) в telegram
        with open(f'{seven_days_ago} - {day_ago}.pdf', 'rb') as pdf_file:
            input_file = InputFile(pdf_file)
            bot.sendDocument(chat_id=chat_id, document=input_file)
            
        return 
    
    # REPORT FOR MESSAGE
    @task
    def report_message():
        # запрос в базу данных
        query = """
                SELECT toDate(time) AS date, 
                COUNT (DISTINCT user_id) AS DAU,
                COUNT (user_id) AS msg_sent,
                new_users.count_new_users AS new_users,
                new_users.count_organic_users AS organic_users,
                new_users.count_ads_users AS ads_users
                FROM message_actions AS ma

                JOIN 
                -- количество новых пользователей, органических, по рекламе
                (SELECT date, COUNT(DISTINCT user_id) AS count_new_users,
                 COUNTIf(DISTINCT user_id, source = 'organic') AS count_organic_users,
                 COUNTIf(DISTINCT user_id, source = 'ads') AS count_ads_users
                FROM (SELECT user_id, MIN(toDate(time)) AS date, source
                      FROM message_actions
                      GROUP BY user_id, source) 
                WHERE date BETWEEN (today() - 7) AND (today() - 1)
                GROUP BY date) AS new_users
                ON toDate(ma.time) = new_users.date

                WHERE toDate(time) BETWEEN (today() - 7) AND (today() - 1)
                GROUP BY date, new_users, organic_users, ads_users
                ORDER BY date DESC
            """
        df = ch_get_df(query)
        
        # формирование текстового сообщения
        msg = f'''*2. Сервис сообщений на {day_ago} (отклонение от {two_days_ago}):*
        
*DAU:* {str(df['DAU'][0])} ({diff(df, 'DAU')} | {percent(df, 'DAU')});
*Количество отправленных сообщений:* 
{str(df['msg_sent'][0])} ({diff(df, 'msg_sent')} | {percent(df, 'msg_sent')});
*Количество новых пользователей:* 
{str(df['new_users'][0])} ({diff(df, 'new_users')} | {percent(df, 'new_users')}), в том числе: 
*- органических:* {str(df['organic_users'][0])} ({diff(df, 'organic_users')} | {percent(df, 'organic_users')});
*- по рекламе:* {str(df['ads_users'][0])} ({diff(df, 'ads_users')} | {percent(df, 'ads_users')}).

*Графики сервиса сообщений с {seven_days_ago} по {day_ago}:*
        '''
        # отправка сообщения
        message(msg)
        
        # создание PdfPages объекта
        pdf_pages_messages = PdfPages(f'{seven_days_ago} - {day_ago}.pdf')
        
        # формирование графиков
        graph(df, 'date', 'DAU', 'DAU', pdf_pages_messages)
        graph(df, 'date', 'msg_sent', 'Количество отправленных сообщений', pdf_pages_messages)
        graph(df, 'date', 'new_users', 'Количество новых пользователей', pdf_pages_messages)
        graph(df, 'date', 'organic_users', 'Количество новых органических пользователей', pdf_pages_messages)
        graph(df, 'date', 'ads_users', 'Количество новых пользователей по рекламе', pdf_pages_messages)
        
        # закрываем PDF файл
        pdf_pages_messages.close()
        
        # открытие и отправка файла (графиков) в telegram
        with open(f'{seven_days_ago} - {day_ago}.pdf', 'rb') as pfd_file:
            input_file = InputFile(pfd_file)
            bot.sendDocument(chat_id=chat_id, document=input_file)
        
        return 
    
        # отправление: отчет для новостной ленты + графики, 
        #              отчет для сервиса сообщений + графики.
        
    report_feed = report_feed()
    report_message = report_message()
    
    report_feed >> report_message
    
n_abramkin_telegram_report = n_abramkin_telegram_report()