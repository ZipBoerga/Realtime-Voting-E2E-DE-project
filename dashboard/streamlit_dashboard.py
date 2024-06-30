import time

import psycopg2
import streamlit as st


@st.cache_data
def fetch_voting_stats() -> (int, int):
    conn = psycopg2.connect(
        database='postgres',
        user='postgres',
        password='postgres',
        host='127.0.0.1',
        port='5432',
    )
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM voters')
    voters_count = cursor.fetchone()[0]

    cursor.execute('SELECT COUNT(*) FROM candidates')
    candidates_count = cursor.fetchone()[0]

    return voters_count, candidates_count


def update_date():
    with st.empty():
        st.write(f'Last refreshed as {time.strftime("%Y-%m-%d %H:%M:%S")}')

    voters_count, candidates_count = fetch_voting_stats()
    st.markdown('''---''')
    col1, col2 = st.columns(2)
    col1.metric('Total voters', voters_count)
    col2.metric('Total candidates', candidates_count)


if __name__ == '__main__':
    st.write('Realtime Election Voting Dashboard')
    update_date()
