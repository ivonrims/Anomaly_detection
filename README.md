# Anomaly detection & Alert system

**Tools:**
- Pandas
- ClickHouse
- ETL-pipeline in Airflow
- Telegram-bot

# Description
We need to write an alert system for our application. The system should check key metrics every 15 minutes - such as active users in the feed/messenger, views, likes, CTR, number of messages sent. If an abnormal value is detected, an alert should be sent to the chat - a message with the following information: the metric, its value, the magnitude of the deviation.

To detect abnormal values, it was decided to use the interquartile range method to construct a confidence interval. The confidence interval is based on the metric quartiles for the last 3 hours (the optimal period, which was determined based on the graphs). If any metric value for the last 15 minutes falls outside this confidence interval, an alert is triggered and the telegram bot sends a message to the chat. 

