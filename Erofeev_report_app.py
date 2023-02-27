from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import io 
import seaborn as sns
import requests
import telegram
import matplotlib.pyplot as plt



from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database': 'simulator_20221220',
                      'user': 'student',
                      'password': 'dpo_python_2020'
                      }
# Дефолтные параметры, которые прокидываются в таски

default_args = {
    'owner': 'p-erofeev',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 5),
    'start_date': datetime(2023, 1, 30),
}

# Интервал запуска DAG

schedule_interval = '0 11 * * *'
@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def dag_report_feed_and_message_erofeev():

    @task()
    def extract():
        query = """SELECT  date, users_message, users_feed, messages, likes, views, CTR
                FROM
                    (SELECT toDate(time) AS date,
                    uniqExact(user_id) AS users_message,
                    COUNT(user_id) AS messages
                    FROM simulator_20221220.message_actions
                    WHERE date >= today() - 7 AND date <= today() - 1
                    GROUP BY date
                     )AS t1
                FULL JOIN
                    (SELECT toDate(time) AS date,
                    uniqExact(user_id) AS users_feed,
                    countIf(user_id, action='view') AS views,
                    countIf(user_id, action='like') AS likes,
                    countIf(action = 'like')/countIf(action = 'view') AS CTR
                    FROM simulator_20221220.feed_actions
                    WHERE date >= today() - 7 AND date <= today() - 1
                    GROUP BY date
                    ) AS t2
                ON t1.date = t2.date  
                 """
        
        df_cube = ph.read_clickhouse(query, connection = connection)
        return df_cube

  
    @task
    def transform_text(df_cube):
        yesterday = datetime.now() - timedelta(days=1)
        date_string = yesterday.strftime('%Y-%m-%d')
        
        ctr = round(df_cube['CTR'][df_cube['date'] == date_string], 3).iloc[0]
        dau_feed = df_cube['users_feed'][df_cube['date'] == date_string].iloc[0]
        dau_messenger = df_cube['users_message'][df_cube['date'] == date_string].iloc[0]
        likes = df_cube['likes'][df_cube['date'] == date_string].iloc[0]
        views = df_cube['views'][df_cube['date'] == date_string].iloc[0]
        messages = df_cube['messages'][df_cube['date'] == date_string].iloc[0]
        
        text = f'Отчёт за {date_string} по ленте и сообщениям:\n DAU ленты = {dau_feed}\n DAU мессенджера = {dau_messenger}\n Просмотры = {views}\n Лайки = {likes}\n CTR = {ctr}\n Сообщения = {messages}\n Графики за прошедшую неделю:'
        
        return text


    @task
    def transform_chart(df_cube):
        figure, axs = plt.subplots(4, 1, figsize = (15, 30))
        sns.lineplot(data = df_cube, x = "date", y = "users_feed", label='Лента', ax = axs[0])
        sns.lineplot(data = df_cube, x = "date", y = "users_message", label='Сообщения', ax = axs[0])
        axs[0].set_title("DAU", fontsize = 16)
        axs[0].set_xlabel("Дата", fontsize = 14)
        axs[0].set_ylabel("Уникальные пользователи", fontsize=14)
        axs[0].grid()

        sns.lineplot(data = df_cube, x = "date", y = "views", label='Просмотры', ax = axs[1])
        sns.lineplot(data = df_cube, x = "date", y = "likes", label='Лайки', ax = axs[1])
        axs[1].set_title("Лайки и просмотры", fontsize = 16)
        axs[1].set_xlabel("Дата", fontsize = 14)
        axs[1].set_ylabel("Количество действий", fontsize = 14)
        axs[1].grid()

        sns.lineplot(data = df_cube, x = "date", y = "CTR", ax = axs[2])
        axs[2].set_title("CTR", fontsize = 16)
        axs[2].set_xlabel("Дата", fontsize = 14)
        axs[2].grid()

        sns.lineplot(data = df_cube, x = "date", y = "messages", ax = axs[3])
        axs[3].set_title("Сообщения", fontsize = 16)
        axs[3].set_xlabel("Дата", fontsize = 14)
        axs[3].set_ylabel("Количество сообщений", fontsize=14)
        axs[3].grid()

        plot_object=io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Графики за неделю'
        plt.close()
        
        return plot_object



    @task
    def load(text, plot_object):
        bot=telegram.Bot(token = '6041309867:AAFvptymKwQi7Ad7qaCpvV_7ExYvsfNtWxU')
        chat_id=-850804180
        
        bot.sendMessage(chat_id = chat_id, text = text)
        bot.sendPhoto(chat_id = chat_id, photo = plot_object)


    df_cube = extract()
    text = transform_text(df_cube)
    plot_object = transform_chart(df_cube)
    load(text, plot_object)


dag_report_feed_and_message_erofeev = dag_report_feed_and_message_erofeev()
