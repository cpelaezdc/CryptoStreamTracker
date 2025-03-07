| APIs | KAFKA | PYTHON | POSTGRESQL | DOCKER | POWERBI |
|---|---|---|---|---|---| 
| Data Source | ETL Pipeline | Scripting | Data Storage | Containerization | Data Visualization |

## Introduction

**CryptoStreamTracker** is a cutting-edge project designed to track and analyze cryptocurrency changes in real-time. By leveraging the power of Apache Kafka, Python, and Power BI, this project offers a robust and scalable solution for real-time data processing and visualization.

## Key Components & Tools

1. **Source Data:**
   - **API Request:** Utilize the **COPINCAP API** to fetch real-time data.
   - **Selection:** Focus on 5 key cryptocurrencies for tracking and analysis.

2. **Data Streaming:**
   - **Kafka:** Employ Apache Kafka to stream the selected cryptocurrency data in real-time.
   - **Python Kafka Client:** Use the Python Kafka Client to implement and manage data streams.

3. **Data Storage:**
   - **CSV Storage:** Save real-time data streams into CSV files for seamless reading and visualization in Power BI.
   - **Batch Processing:** Utilize Apache Airflow to load historical data in batch processes.
   - **Data Warehouse:** Store processed historical data in a Postgres data warehouse for comprehensive analysis.

4. **Docker Containers:**
   - **Kafka, Postgres, and Airflow:** Containerize these components using Docker to ensure efficient deployment and management.


## Arquitecture
![Arquitecture](assets/img/Arquitecture.png)


## Data Model
🚧 **Under Construction** 🚧

![DataModel](assets/img/pending.png)

### Dimension Tables:
🚧 **Under Construction** 🚧

### Factual Tables:
🚧 **Under Construction** 🚧

## Environments
🚧 **Under Construction** 🚧


## Scripts
For this project, we utilize Python along with libraries such as Confluent Kafka Python and Pandas.


## Visualisations
🚧 **Under Construction** 🚧


## Pipeline general view
🚧 **Under Construction** 🚧


## Dataset Used
*  See APIs - [datasets](https://docs.coincap.io/)

## Data Sources
- [Coincap](https://coincap.io/)

