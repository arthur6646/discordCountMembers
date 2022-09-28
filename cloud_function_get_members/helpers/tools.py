import discord
from discord.ext import commands
from datetime import datetime
import pandas as pd 
import io 
from google.cloud import storage # Google Cloud Storage
client = storage.Client()
import datetime


def get_minutes_hours(df) :

    current_time = datetime.datetime.now()
    df["hora_coletado"] = current_time.hour
    df["minuto_coletado"] = current_time.minute
    return df 

def dtProcessamento_option(df,how="by_today_date",filename = None,date=None):
    """
    Adiciona a data de processamento dos dados ao dataframe existente
    """



    if how == "by_filename" :
        datas = filename.split('_')[-1].split('.xlsx')[0]
        data = f'{datas[0:4]}-{datas[4:6]}-{datas[6:]}'
    elif how == "by_fix_date" :
        data = date
    elif how == "by_today_date" :
        data = datetime.date(datetime.today())
    df["dtProcessamento"] = data
    df['dtProcessamento'] = pd.to_datetime(df["dtProcessamento"]).dt.date
    return df





def df_to_gcs(bucket_gcs,filepath,dataframe):
    """"
    Pega os dados em formato de dataframe e salva no bucket do google em formato parquet
    """

    io_file = io.BytesIO()
    dataframe.to_parquet(io_file, index = False)     
    buckets = client.get_bucket(bucket_gcs)
    blob = buckets.blob(filepath)
    blob.upload_from_string(io_file.getvalue(), content_type='application/octet-stream')






def get_info(token) :
    
    """
    A partir do token do Discord, pega as informações dos servidores e a contagem 
    de usuarios de cada discord onde o bot esta presente

    """
    # enable discord gateway intents
    intents = discord.Intents.default()
    intents.members = True

    guilds =[]
    membercounts = []
    membersNames = []

    @bot.event
    
    async def on_ready(): 
        """ Runs once the bot has established a connection with Discord """
        print(f'{bot.user.name} has connected to Discord')

        # check if bot has connected to guilds
        if len(bot.guilds) > 0:
            print('connected to the following guilds:')

            # list guilds
            for guild in bot.guilds:
                guilds.append(guild.name)
                membercounts.append(len(guild.members))
                membersNames.append([[m.id,m.name,m.bot] for m in guild.members])
                
                # display guild name, id and member count
                print(f'* {guild.name}#{guild.id}, member count: {len(guild.members)}')
                for members in guild.members :
                    print(members)
                    
        bot.clear()
        await bot.close()
    
    bot.run(token)

    return  guilds,membercounts,membersNames

def info_to_df(servers,qtdn,members) : 

    dict_example = {"nome_servidor" : servers,"Quantidade " : qtdn}

    df = pd.DataFrame.from_dict(dict_example)
    return df
