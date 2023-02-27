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
def dag_report_erofeev():

    @task()
    def extract():
        query = """SELECT toDate(time) AS event_date,
                    uniq(user_id) AS uniq_users,
                    countIf(action = 'like')/countIf(action = 'view') AS ctr,
                    countIf(action = 'like') AS likes,
                    countIf(action = 'view') AS views
                FROM simulator_20221220.feed_actions
                WHERE event_date<=today()-1 AND event_date>=today()-7 
                GROUP BY event_date
                ORDER BY event_date
                 """
        
        df_cube = ph.read_clickhouse(query, connection = connection)
        return df_cube

  
    @task
    def transform_text(df_cube):
        yesterday = datetime.now() - timedelta(days=1)
        date_string = yesterday.strftime('%Y-%m-%d')
        
        ctr = round(df_cube['ctr'][df_cube['event_date'] == date_string], 3).iloc[0]
        dau = df_cube['uniq_users'][df_cube['event_date'] == date_string].iloc[0]
        likes = df_cube['likes'][df_cube['event_date'] == date_string].iloc[0]
        views = df_cube['views'][df_cube['event_date'] == date_string].iloc[0]
        
        text=f'Отчёт за {date_string}:\n DAU={dau}\n Просмотры={views}\n Лайки={likes}\n CTR={ctr}\n Графики за прошедшую неделю:'
        
        return text


    @task
    def transform_chart(df_cube):
        figure, axs = plt.subplots(3, 1, figsize = (15, 20))
        sns.lineplot(data = df_cube, x = "event_date", y = "uniq_users", ax = axs[0])
        axs[0].set_title("DAU", fontsize = 16)
        axs[0].set_xlabel("Дата", fontsize = 14)
        
        sns.lineplot(data = df_cube, x = "event_date", y = "views", label='Просмотры', ax = axs[1])
        sns.lineplot(data = df_cube, x = "event_date", y = "likes", label='Лайки', ax = axs[1])
        axs[1].set_title("Лайки и просмотры", fontsize = 16)
        axs[1].set_xlabel("Дата", fontsize = 14)
        axs[1].set_ylabel("Количество действий", fontsize = 14)
 
        sns.lineplot(data=df_cube, x = "event_date", y = "ctr", ax = axs[2])
        axs[2].set_title("CTR", fontsize = 16)
        axs[2].set_xlabel("Дата", fontsize = 14)
        


        plot_object=io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Графики'
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


dag_report_erofeev = dag_report_erofeev()
