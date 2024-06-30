import time

import psycopg2
import streamlit as st
import simplejson as json
import pandas as pd
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from streamlit_autorefresh import st_autorefresh


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


def create_kafka_consumer(topic: str):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )
    return consumer


def plot_bar_chart(data):
    split_col = data['candidate_name']
    numbers = data['total_votes']

    cmap = plt.get_cmap('viridis')
    colors = [cmap(i / len(split_col)) for i in range(len(split_col))]

    plt.bar(split_col, numbers, color=colors)
    plt.xlabel('Candidates')
    plt.ylabel('Total Votes')
    plt.title('Vote Counts per Candidate')
    plt.xticks(rotation=90)
    return plt


def plot_donut_chart(data: pd.DataFrame):
    labels = list(data['candidate_name'])
    sizes = list(data['total_votes'])

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    plt.title('Vote Counts per Candidate')
    return fig


def display_sidebar():
    if st.session_state.get('latest_update') is None:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider('Refresh interval (seconds)', 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key='auto')

    if st.sidebar.button('Refresh Data'):
        update_date()


def display_general_stats():
    voters_count, candidates_count = fetch_voting_stats()
    st.markdown('''---''')
    col1, col2 = st.columns(2)
    col1.metric('Total voters', voters_count)
    col2.metric('Total candidates', candidates_count)


def get_data_from_kafka(topic: str) -> pd.DataFrame:
    consumer = create_kafka_consumer(topic)
    messages = consumer.poll(timeout_ms=1000)
    data = []
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return pd.DataFrame(data)


def display_leading_candidate(df: pd.DataFrame) -> None:
    leading_candidate = df.loc[df['total_votes'].idxmax()]

    st.markdown('''---''')
    st.header('Leading Candidate')
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader('Total Vote: {}'.format(leading_candidate['total_votes']))


def display_general_voting_stats(df: pd.DataFrame) -> None:
    st.markdown('''---''')
    st.header('Voting Statistics')

    df = df[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]

    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_bar_chart(df)
        st.pyplot(bar_fig)
    with col2:
        bar_fig = plot_donut_chart(df)
        st.pyplot(bar_fig)


## I didn't really get into this part of code and just copy pasted it,
# it is not that interesting as the general infrastracture
def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=1, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio("Direction", options=["⬆️", "⬇️"], horizontal=True)
        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
        )
    pagination = st.container()

    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = (
            int(len(table_data) / batch_size)
            if int(len(table_data) / batch_size) > 0
            else 1
        )
        current_page = st.number_input(
            "Page", min_value=1, max_value=total_pages, step=1
        )
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}** ")

    pages = [
        table_data.loc[i : i + batch_size - 1, :]
        for i in range(0, len(table_data), batch_size)
    ]
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)


def update_date():
    with st.empty():
        st.write(f'Last refreshed at {time.strftime("%Y-%m-%d %H:%M:%S")}')

    display_general_stats()

    votes_per_candidate_df = get_data_from_kafka('votes_per_candidate')

    if votes_per_candidate_df.shape[0] != 0:
        votes_per_candidate_df = votes_per_candidate_df.loc[
            votes_per_candidate_df.groupby('candidate_id')['total_votes'].idxmax()
        ].reset_index(drop=True)
        display_leading_candidate(votes_per_candidate_df)
    else:
        st.text('No data on voting per candidates yet')

    turnout_by_location_df = get_data_from_kafka('turnout_by_location')
    if turnout_by_location_df.shape[0] != 0:
        turnout_by_location_df = turnout_by_location_df.loc[
            turnout_by_location_df.groupby('state')['count'].idxmax()
        ].reset_index(drop=True)

        st.markdown('''---''')
        st.header('Turnout by location')
        paginate_table(turnout_by_location_df)
        st.session_state['last_update'] = time.time()
    else:
        st.text('No data on voting per location yet')


if __name__ == '__main__':
    st.write('Realtime Election Voting Dashboard')
    display_sidebar()
    update_date()
