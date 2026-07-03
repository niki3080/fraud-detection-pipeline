# Shad MTS MLOps Project 2

Это учебный проект построения системы обнаружения мошеннических транзакций на основе потоковых данных с использованием Kafka, PostgreSQL, Streamlit UI и ML-модели (CatBoost). Проект реализует полный цикл обработки данных от загрузки до визуализации результатов.

Датасет можно найти [здесь](https://www.kaggle.com/competitions/teta-ml-1-2025/data).


## В проекте реализовано:

- Kafka consumer/producer для чтения транзакций и записи результатов скоринга: `fraud_detector/app/app.py`
- Препроцессинг данных: `fraud_detector/src/preprocessing.py`
- Скоринг обработанных сообщений: `fraud_detector/src/scorer.py`
- Запись результатов скоринга из Kafka в PostgreSQL: `scoring_writer/app.py`
- Streamlit UI для отправки CSV-файла в Kafka и просмотра результатов из PostgreSQL: `interface/app.py`
- В Streamlit UI добавлен раздел просмотра результатов по кнопке "Посмотреть результаты":  
  - выводятся 10 последних транзакций из PostgreSQL с fraud_flag=1;
  - строится гистограмма распределения скоров последних 100 транзакций из PostgreSQL.
- В Grafana добавлен аналитический дашборд с фильтрами по _us_state_, _merch_, _cat_id_  
  Дашборд: `grafana/dashboards/fraud_scoring_analytics.json`
- Все сервисы поднимаются через Docker Compose  
  Конфиг: `docker-compose.yaml`


## Grafana dashboard

В проект добавлен дашборд **Fraud Scoring Analytics**. Структура дашборда:

- фильтры по `us_state`, `merch` и `cat_id`;
- распределение скоров модели;
- TPS обработки транзакций;
- barplot средней доли fraud-транзакций по `cat_id` в последних 1000 транзакций;
- сводные метрики по количеству транзакций и доле fraud-транзакций;
- таблица последних fraud-транзакций;
- top-10 штатов по доле fraud-транзакций.

Дашборд _Fraud Scoring Analytics_ в Grafana использует данные из таблицы `scores` PostgreSQL.

## Структура PostgreSQL `scores`

| Поле             | Тип          | Описание                                |
|------------------|--------------|-----------------------------------------|
| id               | SERIAL       | Внутренний идентификатор записи         |
| transaction_id   | TEXT         | Уникальный идентификатор транзакции     |
| score            | FLOAT        | Скор модели, вероятность fraud-класса   |
| fraud_flag       | INT          | 1 – мошенничество, 0 – норма            |
| us_state         | TEXT         | Штат транзакции                         |
| merch            | TEXT         | Мерчант                                 |
| cat_id           | TEXT         | Категория продукта                      |
| created_at       | TIMESTAMP    | Дата и время записи                     |


## Модель

Для расчета скоров используется предобученная модель CatBoost Classifier.

- Обучение модели внутри контейнера не выполняется, сервис работает только в режиме inference.
- Загрузка обучающих данных для запуска сервиса не требуется.
- Модель хранится в файле `fraud_detector/models/catboost_fraud_model.cbm`.
- Параметры препроцессинга хранятся в файле `fraud_detector/models/preprocessing_config.json`.
- Конфигурация модели хранится в файле `fraud_detector/models/model_config.json`.
- Порог классификации `threshold` берется из `model_config.json` и используется для расчета `fraud_flag`.
- Inference выполняется на CPU.


## Как запустить проект

1. Склонировать репозиторий:

   ```bash
   git clone https://github.com/niki3080/fraud-detection-streaming.git
   cd fraud-detection-streaming
   ```

2. Скопировать переменные окружения:

   ```bash
   cp .env.example .env
   ```

3. Собрать и запустить контейнеры:

   ```bash
   docker compose up --build
   ```

4. Открыть Streamlit UI:

   [http://localhost:8501](http://localhost:8501)

   Для проверки работы сервиса можно загрузить пример файла `input/batch1.csv` и нажать кнопку отправки.

5. Открыть Grafana:

   [http://localhost:3000](http://localhost:3000)

   Логин: `admin`  
   Пароль: `admin`

   Основной дашборд проекта:  
   [Fraud Scoring Analytics](http://localhost:3000/d/adkqp9c/fraud-scoring-analytics)

6. Дополнительные сервисы:

   | Сервис | URL |
   |--------|-----|
   | Kafka UI | [http://localhost:8080](http://localhost:8080) |
   | Prometheus | [http://localhost:9090](http://localhost:9090) |

<p>&nbsp;</p>

---

<p>&nbsp;</p>

## Архитектура

Проект состоит из следующих сервисов:

| Сервис           | Описание |
|------------------|----------|
| **Kafka**        | Шина сообщений для потоковой передачи данных между компонентами |
| **Zookeeper**    | Управление кластером Kafka |
| **Kafka UI**     | Веб-интерфейс для мониторинга Kafka |
| **kafka-setup** | Сервис для автоматического создания Kafka-топиков `transactions` и `scoring` |
| **fraud_detector** | Микросервис, выполняющий предобработку данных и инференс модели CatBoost |
| **scoring_writer** | Потребитель Kafka, сохраняет результаты скоринга в PostgreSQL |
| **PostgreSQL**   | Хранилище результатов скоринга |
| **interface**    | Веб-интерфейс (Streamlit) для загрузки файлов и просмотра результатов |
| **Prometheus**   | Система мониторинга и сбора метрик |
| **Grafana**      | Визуализация метрик и создание дашбордов |
| **Node Exporter**| Сбор метрик о состоянии системы (CPU, память и т.д.) |

---

## Структура проекта

```text
.
├── fraud_detector/           # Микросервис скоринга транзакций
│   ├── app/                  # Kafka consumer/producer
│   ├── models/               # Модель CatBoost и конфиги препроцессинга
│   ├── src/                  # Препроцессинг и скоринг
│   ├── Dockerfile
│   └── requirements.txt
├── interface/                # Веб-интерфейс Streamlit
│   ├── app.py
│   ├── plot_score_density.py
│   ├── Dockerfile
│   └── requirements.txt
├── scoring_writer/           # Сервис записи результатов в PostgreSQL
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── input/                    # Пример входного CSV-файла
│   └── batch1.csv
├── prometheus/               # Конфигурация Prometheus
│   └── prometheus.yml
├── grafana/                  # Provisioning и дашборды Grafana
│   ├── provisioning/
│   └── dashboards/
├── docker-compose.yaml       # Основной файл Docker Compose
├── .env.example              # Пример файла переменных окружения
├── .gitignore
└── README.md
```

---

## Функциональность

### 1. Загрузка CSV-файла с транзакциями
- Поддерживается только формат `.csv`.
- Каждая строка отправляется в Kafka топик `transactions` с уникальным `transaction_id`.

### 2. Обработка и скоринг транзакций
- Сервис `fraud_detector` считывает данные из топика `transactions`.
- Выполняет препроцессинг и предсказание с помощью модели CatBoost.
- Результат (`score`, `fraud_flag`) отправляется в топик `scoring`.

### 3. Хранение результатов
- Сервис `scoring_writer` читает топик `scoring`.
- Сохраняет результаты в PostgreSQL таблицу `scores`.

### 4. Визуализация результатов
- Через кнопку "Посмотреть результаты" можно:
  - Посмотреть **10 последних фродовых транзакций** (`fraud_flag == 1`)
  - Посмотреть **гистограмму распределения скоров** по последним 100 транзакциям

### 5. Мониторинг и метрики
- Система собирает метрики о работе сервисов и железа
- Доступны дашборды для визуализации:
  - Метрик модели (скоры предсказания, распределение скоров)
  - Производительности (время обработки транзакций, скорость записи в БД)
  - Системных ресурсов (CPU, RAM, диск, сеть)
  - Аналитики скоринга с фильтрами по штатам, мерчантам и категориям продукта

---

## Технологии

- **Python 3.9+**
- **ML**: CatBoost, pandas, numpy
- **Kafka**: потоковая обработка данных
- **PostgreSQL**: хранение результатов
- **Streamlit**: визуализация и пользовательский интерфейс
- **Docker / Docker Compose**: оркестрация микросервисов
- **Prometheus**: сбор и хранение метрик
- **Grafana**: визуализация метрик в виде дашбордов

---

### Очистка после тестирования

Остановите и удалите контейнеры:
```bash
docker compose down -v
```

Если нужно очистить данные и метрики:
```bash
docker volume rm $(docker volume ls -q | grep -E 'postgres_data|prometheus_data|grafana_data')
```