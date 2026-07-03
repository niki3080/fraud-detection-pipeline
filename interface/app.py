import json
import os
import time
import uuid

import pandas as pd
import psycopg2
import streamlit as st
from kafka import KafkaProducer

from plot_score_density import build_score_density_plot


# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions"),
}

MODEL_CONFIG_PATH = os.getenv("MODEL_CONFIG_PATH", "/app/model_config.json")


def load_model_threshold():
    with open(MODEL_CONFIG_PATH) as f:
        return json.load(f)["threshold"]
    

def load_file(uploaded_file_):
    """Загрузка CSV файла в DataFrame."""
    try:
        return pd.read_csv(uploaded_file_)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {e!s}")
        return None


def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT",
        )

        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

        progress_bar = st.progress(0)
        total_rows = len(df)

        for idx, row in df.iterrows():
            # Отправляем данные вместе с ID
            producer.send(
                topic,
                value={
                    "transaction_id": row['transaction_id'],
                    "data": row.drop('transaction_id').to_dict(),
                },
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)

        producer.flush()

        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {e!s}")
        return False


# Функция подключения к БД
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# Загрузка 10 последних фродовых транзакций
def load_fraud_transactions(limit=10):
    conn = get_db_connection()
    query = f"""
        SELECT * FROM scores WHERE fraud_flag = 1
        ORDER BY created_at DESC LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# Загрузка 100 последних транзакций
def load_scores(limit=100):
    conn = get_db_connection()
    query = f"""
        SELECT score FROM scores
        ORDER BY created_at DESC LIMIT {limit}
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"],
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    # Добавляем файл в состояние
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file),
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")

    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])

        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")

        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"],
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")

has_pending_files = any(
    file_data["status"] != "Отправлен"
    for file_data in st.session_state.uploaded_files.values()
)

if has_pending_files:
    st.info(
        "Если файл сейчас отправляется, дождитесь завершения перед просмотром результатов: "
        "нажатие кнопки обновляет страницу и может остановить отправку."
    )

if st.button("Посмотреть результаты"):
    st.subheader("Последние 10 фродовых транзакций:")
    fraud_df = load_fraud_transactions(limit=10)
    if not fraud_df.empty:
        st.dataframe(fraud_df[["transaction_id", "score", "fraud_flag", "created_at"]])
    else:
        st.write("Нет записей с fraud_flag == 1")

    st.subheader("Гистограмма скоров последних транзакций:")
    score_df = load_scores(limit=100)
    if not score_df.empty:
        threshold = load_model_threshold()
        fig = build_score_density_plot(score_df["score"], threshold=threshold)
        st.pyplot(fig, use_container_width=True)
    else:
        st.write("Нет записей в базе для построения гистограммы")
