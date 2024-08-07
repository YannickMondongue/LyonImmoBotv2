# openai
import streamlit as st
from openai import OpenAI
from sqlalchemy import create_engine

client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

completion = client.chat.completions.create(
    model="gpt-3.5-turbo",
    messages=[
        {"role": "user", "content": "What is Streamlit?"}
    ]
)

st.write(completion.choices[0].message.content)


# postgresql
from sqlalchemy import create_engine
import pandas as pd

conn_str = (
    f"postgresql+psycopg2://{st.secrets['connections']['postgresql']['user']}:"
    f"{st.secrets['connections']['postgresql']['password']}@"
    f"{st.secrets['connections']['postgresql']['host']}:"
    f"{st.secrets['connections']['postgresql']['port']}/"
    f"{st.secrets['connections']['postgresql']['database']}"
)

engine = create_engine(conn_str)
with engine.connect() as connection:
    df = pd.read_sql_query("SELECT current_database()", connection)

st.write(df)




