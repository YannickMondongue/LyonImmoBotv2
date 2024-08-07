import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from prompts import get_system_prompt
from openai import OpenAI
import re

st.title("🏠 LyonImmoBot")

# Initialize the chat messages history
client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "system", "content": get_system_prompt()}]

# Prompt for user input and save
if prompt := st.chat_input():
    st.session_state.messages.append({"role": "user", "content": prompt})

# display the existing chat messages
for message in st.session_state.messages:
    if message["role"] == "system":
        continue
    with st.chat_message(message["role"]):
        st.write(message["content"])
        if "results" in message:
            st.dataframe(message["results"])
            
# Function to execute SQL query with error handling
def execute_query(query: str):
    try:
        conn_str = (
            f"postgresql+psycopg2://{st.secrets['connections']['postgresql']['user']}:"
            f"{st.secrets['connections']['postgresql']['password']}@"
            f"{st.secrets['connections']['postgresql']['host']}:"
            f"{st.secrets['connections']['postgresql']['port']}/"
            f"{st.secrets['connections']['postgresql']['database']}"
        )
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            result = connection.execute(text(query))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        st.error("Désolé, une erreur est survenue, veuillez ré-écrire la question. Si l’erreur persiste, reformulez avec plus de contexte.")
        # Log the error for debugging purposes
        print(f"Erreur lors de l'exécution de la requête : {e}")
        return None

# If last message is not from assistant, we need to generate a new response
if st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):
        response = ""
        resp_container = st.empty()
        for delta in client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
            stream=True,
        ):
            response += (delta.choices[0].delta.content or "")
            resp_container.markdown(response)

        message = {"role": "assistant", "content": response}
        # Parse the response for a SQL query and execute if available
        sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)
        if sql_match:
            sql = sql_match.group(1).strip()
            conn_str = (
                f"postgresql+psycopg2://{st.secrets['connections']['postgresql']['user']}:"
                f"{st.secrets['connections']['postgresql']['password']}@"
                f"{st.secrets['connections']['postgresql']['host']}:"
                f"{st.secrets['connections']['postgresql']['port']}/"
                f"{st.secrets['connections']['postgresql']['database']}"
            )
            engine = create_engine(conn_str)
            with engine.connect() as connection:
                try:
                    result = connection.execute(text(sql))
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    message["results"] = df
                    st.dataframe(message["results"])
                except Exception as e:
                    st.error(f"An error occurred: {e}")
        st.session_state.messages.append(message)
