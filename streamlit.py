import streamlit as st
import snowflake.connector

st.title('❄️ How to connect Streamlit to a Snowflake database')

# Establish Snowflake connection
@st.cache(allow_output_mutation=True)
def create_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["bharathteja"],
        password=st.secrets["snowflake"]["Bharathteja1234"],
        account=st.secrets["snowflake"]["UDVIGOL.QZ79082"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["PETS"],
        schema=st.secrets["snowflake"]["schema"]
    )

conn = create_connection()
st.success("Connected to Snowflake!")

# Execute SQL query and display results
@st.cache(allow_output_mutation=True)
def execute_query(query):
    return conn.cursor().execute(query)

# Example query
query = "SELECT * FROM PETS.PUBLIC.MYTABLE LIMIT 100"

with st.expander("See Table"):
    cursor = execute_query(query)
    rows = cursor.fetchall()
    st.write(rows)

# Writing out data
for row in rows:
    st.write(f"{row[0]} has a :{row[1]}:")
