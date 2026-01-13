# ev-charging-pipeline

This project collects, processes, and analyzes public electric vehicle (EV) charging station data from the Transport for London (TfL) API. The data is transformed into a structured dataset suitable for reporting, analytics, and visualization.

## Overview

The pipeline retrieves information about:
- EV charging stations
- Connectors
- Power characteristics
- Status and availability
- Geographic distribution
- Operator details

After processing, the data is organized into a clean analytical dataset that can be used to support reporting, dashboards, or research related to EV infrastructure in London.

## Features

- Fetches live data from the TfL API
- Normalizes and enriches station and connector information
- Produces a curated analytics model for downstream use
- Designed to support dashboards and BI tools
- Extensible and modular pipeline design

## Use Cases

This dataset can support:
- Infrastructure planning
- Operator network comparisons
- Availability monitoring
- Urban mobility research
- Fleet electrification strategy
- Public policy and sustainability insights

## Data Outputs

The final processed dataset provides attributes such as:
- Station location (latitude/longitude)
- Connector type and power rating
- Status (available, in-use, etc.)
- Speed category (rapid, fast, slow)
- Operator name
- Geographic filters (e.g., Central London)
- Snapshot timestamps for historical comparison

## Technology

The pipeline can be executed using:
- Python
- Cloud orchestration
- BigQuery
- Looker Studio

No specific infrastructure is required to understand or extend the project.