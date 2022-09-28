#Importando as bibliotecas e as funções auxiliares que precisamos para fazer o sistema rodar.
from helpers.tools import get_info,dtProcessamento_option,info_to_df,df_to_gcs,get_minutes_hours
import time 
import datetime



DISCORD_TOKEN = "your-discord-bot-token-here"



def get_members_info(requests) :
    current_time = datetime.datetime.now() #Pegar o timestamp 

    #utiliza o token do discord para coletar a quantidade de pessoas em cada server
    #do discord que o bot esta dispónivel
    servers,quantidades,users_info = get_info(DISCORD_TOKEN) 


    df = info_to_df(servers,quantidades,users_info) # Transforma os dados em um dataframe
    df = dtProcessamento_option(df) # adiciona data de processamento
    df = get_minutes_hours(df) # adiciona minutos e segundos de quando a data foi coletada


    df_to_gcs("your-bucket-here",f"caminho-para-salvar-o-arquivo-{current_time}.parquet",df)



    return "Finalizado!!!!!"
