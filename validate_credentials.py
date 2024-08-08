# openai
import streamlit as st
from openai import OpenAI
from sqlalchemy import create_engine
import pandas as pd

client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

completion = client.chat.completions.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "user", "content": "What is Streamlit?"}
    ]
)

st.write(completion.choices[0].message.content)

# SQL Server
from sqlalchemy import create_engine

# Remplacer la connexion PostgreSQL par SQL Server
conn_str = (
    f"mssql+pyodbc://{st.secrets['connections']['sqlserver']['user']}:"
    f"{st.secrets['connections']['sqlserver']['password']}@"
    f"{st.secrets['connections']['sqlserver']['host']}:1433/"
    f"{st.secrets['connections']['sqlserver']['database']}?"
    "driver=ODBC+Driver+17+for+SQL+Server"
)

engine = create_engine(conn_str)
with engine.connect() as connection:
    df = pd.read_sql_query("SELECT DB_NAME() AS current_database", connection)

st.write(df)

