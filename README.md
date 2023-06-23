# kzr_snowflake
Make it easier to work with snowflake


pypi url - https://pypi.org/project/kzr-snowflake/

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Examples](#examples)

## Overview

Snowflake Python Helper is a Python library designed to simplify interaction with Snowflake databases. It provides an abstracted layer of functions and classes to perform common tasks such as connecting to a Snowflake database, executing queries, manipulating data, and more.

## Features

- Simplified connection to Snowflake database.
- Execution of SQL statements.
- Convenient data selection and insertion methods.
- Time measurement for SQL statement execution.
- DataFrame support for data handling.
- ETL support to transform SQL scripts from source code to executable statements.
- Task creation for scheduling or executing tasks in Snowflake.

## Prerequisites

Before you begin, ensure you have met the following requirements:

- You have installed the required Python libraries: snowflake-connector-python, pandas, numpy, pyarrow, and sqlalchemy.
- You have a Snowflake account.

## Installation

1. Install the required Python libraries:

```shell
pip install -r https://raw.githubusercontent.com/snowflakedb/snowflake-connector-python/v2.5.0/tested_requirements/requirements_36.reqs
pip install snowflake-connector-python==2.5.0
```

2. Install the library using pip
```shell
pip install kzr-snowflake
```
or Clone this repository to your local machine or download the Python script.

## Usage
To use Snowflake Python Helper, you first need to import it in your Python script:

```python
from kzr_snowflake import kzr_snowflake as ks
```

## Examples
1. Connect to your Snowflake database:
```python
ks.path = "/path/to/your/credentials.json"  # replace with your JSON credentials file
ks.role = "YOUR_ROLE"
ks.warehouse = "YOUR_WAREHOUSE"
ks.database = "YOUR_DATABASE"
ks.schema = "YOUR_SCHEMA"
ks.connect()
```

To connect with Shibboleth, use the following code while connecting. But first, make sure your credentials.json file has a Shibboleth user name. No password is needed for this way.
```python
ks.connect(True)
```

example of credentials.json file

```json
{"user": "<YOUR_USER_NAME>", "password": "<YOUR_PASSWORD>", "account": "<ACCOUNT_NAME>"}
```

example of an account name
```
xx12345.us-east-2.aws
```

2. Execute a SQL statement:
```python
sql_statement = "SELECT * FROM YOUR_TABLE"  # replace with your SQL statement
ks.execute(sql_statement)
```

3. Disconnect from the database:
```python
ks.disconnect()
```
4. Select data from a table into a DataFrame:
```python
sql_statement = "SELECT * FROM YOUR_TABLE"  # replace with your SQL statement
df = ks.select_into_df(sql_statement)
```
5. Insert data from a DataFrame into a table:
```python
table_name = "YOUR_TABLE"  # replace with your table name
ks.insert_df(table_name, df)
```

Note: Replace YOUR_ROLE, YOUR_WAREHOUSE, YOUR_DATABASE, YOUR_SCHEMA, YOUR_TABLE, and df with your own values.
