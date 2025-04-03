# Overview

The project utilized AWS Kinesis to capture streaming data from numerous sources. The data was routed to two destinations: AWS Firehose for continuous ingestion and loading into Elasticsearch and an S3 bucket for storing historical data. Elasticsearch processed the Firehose output, enabling filtered results to be visualized in realtime using Kibana dashboards. For historical analysis, AWS Glue crawlers cataloged the data stored in the S3 bucket, and Amazon Athena provided a SQL-like interface for querying. Finally, Tableau was integrated with Athena to create comprehensive data visualizations for deeper insights.

# Data Pipeline

![Pipeline](https://github.com/user-attachments/assets/12b1f840-2256-4b65-8b78-2cdcda9cfd7c)

### Access the full report using the following link.

<a href="https://github.com/Saiyan-AR/Real-Time-Data-Analysis-using-AWS/blob/main/Advanced%20Database%20Project%20Report.pdf" target="_blank">Open Report</a>

###### NOTE: If the report doesn't open, try refreshing the page and open again.
