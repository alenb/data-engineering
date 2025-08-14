# Metro Gold Layer: Star Schema Diagram

Below is an ASCII diagram representing the star schema implemented in the Gold layer. This schema is designed to support efficient analytics and reporting for public transit data.

```
                +-------------------+
                |   dim_date        |
                +-------------------+
                        |
                        |
                        v
+---------+     +-------------------+     +-------------+
|         |<----|   fact_train      |---->| dim_train   |
|         |     +-------------------+     +-------------+
|         |            ^   ^   ^   ^            ^
|         |            |   |   |   |            |
|         |            |   |   |   |            |
|         v            v   v   v   v            v
+-------------+   +-------------+   +-------------+
| dim_station |   | dim_line    |   | dim_mode    |
+-------------+   +-------------+   +-------------+
        ^                ^                ^
        |                |                |
        v                v                v
+-------------------+   +-------------------+
| dim_direction     |   | dim_time_bucket   |
+-------------------+   +-------------------+
```

## Schema Explanation

At the centre of the star schema is the `fact_train` table, which records each train's stop event, including passenger counts, scheduled times, and keys to all relevant dimensions. Surrounding it are dimension tables:

- **dim_date**: Contains calendar information for each business date, including day of week, type, and date key.
- **dim_train**: Describes each train, including its number, mode, line, direction, and endpoints.
- **dim_station**: Details about each station, such as name, latitude, longitude, and chainage.
- **dim_line**: Information about train lines and their groupings.
- **dim_mode**: The mode of transport (e.g., train, tram).
- **dim_direction**: Direction of travel, with descriptions for clarity.
- **dim_time_bucket**: Time intervals for arrivals and departures, supporting time-based analysis.

The fact table links to each dimension via surrogate keys, enabling fast joins and flexible queries. This structure supports slicing and dicing the data by any combination of date, train, station, line, mode, direction, or time bucket—ideal for business intelligence and operational reporting.

## Table Column Breakdown

Below are the columns and data types for each table in the Metro Gold Layer star schema, based on the current pipeline:

### fact_train
| Column                    | Data Type   | Description                                 |
|---------------------------|------------|---------------------------------------------|
| Fact_ID                   | BIGINT     | Surrogate key (primary)                     |
| Date_Key                  | INT        | Foreign key to dim_date                     |
| Train_Key                 | INT        | Foreign key to dim_train                    |
| Station_Key               | INT        | Foreign key to dim_station                  |
| Line_Key                  | INT        | Foreign key to dim_line                     |
| Direction_Key             | INT        | Foreign key to dim_direction                |
| Mode_Key                  | INT        | Foreign key to dim_mode                     |
| Stop_Sequence_Number      | INT        | Sequence of stop for the train              |
| Arrival_Time_Scheduled    | TIMESTAMP  | Scheduled arrival time                      |
| Departure_Time_Scheduled  | TIMESTAMP  | Scheduled departure time                    |
| Arrival_Time_Bucket_Key   | INT        | Foreign key to dim_time_bucket (arrival)    |
| Departure_Time_Bucket_Key | INT        | Foreign key to dim_time_bucket (departure)  |
| Passenger_Boardings       | INT        | Number of passengers boarding               |
| Passenger_Alightings      | INT        | Number of passengers alighting              |
| Passenger_Arrival_Load    | INT        | Passenger load after arrival                |
| Passenger_Departure_Load  | INT        | Passenger load after departure              |

### dim_date
| Column         | Data Type | Description                       |
|----------------|----------|-----------------------------------|
| Date_Key       | INT      | Surrogate key (primary)           |
| Business_Date  | DATE     | Calendar date                     |
| Day_of_Week    | VARCHAR  | Day of week (e.g., Monday)        |
| Day_Type       | VARCHAR  | Type of day (e.g., Business, Holiday) |
| year           | INT      | Year                              |
| month          | INT      | Month number                      |
| day            | INT      | Day of month                      |

### dim_train
| Column             | Data Type | Description                       |
|--------------------|----------|-----------------------------------|
| Train_Key          | INT      | Surrogate key (primary)           |
| Train_Number       | VARCHAR  | Train number                      |
| Mode               | VARCHAR  | Mode of transport                 |
| Line_Name          | VARCHAR  | Line name                         |
| Group              | VARCHAR  | Line group                        |
| Direction          | VARCHAR  | Direction code                    |
| Origin_Station     | VARCHAR  | Start station                     |
| Destination_Station| VARCHAR  | End station                       |

### dim_station
| Column            | Data Type | Description                       |
|-------------------|----------|-----------------------------------|
| Station_Key       | INT      | Surrogate key (primary)           |
| Station_Name      | VARCHAR  | Station name                      |
| Station_Latitude  | FLOAT    | Latitude                          |
| Station_Longitude | FLOAT    | Longitude                         |
| Station_Chainage  | FLOAT    | Distance from reference point     |

### dim_line
| Column    | Data Type | Description                       |
|-----------|----------|-----------------------------------|
| Line_Key  | INT      | Surrogate key (primary)           |
| Line_Name | VARCHAR  | Line name                         |
| Group     | VARCHAR  | Line group                        |

### dim_mode
| Column    | Data Type | Description                       |
|-----------|----------|-----------------------------------|
| Mode_Key  | INT      | Surrogate key (primary)           |
| Mode      | VARCHAR  | Mode name (e.g., Train, Tram)     |

### dim_direction
| Column                | Data Type | Description                                 |
|-----------------------|----------|---------------------------------------------|
| Direction_Key         | INT      | Surrogate key (primary)                     |
| Direction             | VARCHAR  | Direction code                              |
| Direction_Description | VARCHAR  | Direction description                       |

### dim_time_bucket
| Column                | Data Type | Description                                 |
|-----------------------|----------|---------------------------------------------|
| Time_Bucket_Key       | INT      | Surrogate key (primary)                     |
| Time_Bucket           | INT      | Time bucket value (minutes since midnight)  |
| Time_Range_Description| VARCHAR  | Interval label (e.g., 08:00 - 08:29)        |