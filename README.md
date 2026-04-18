# MongoDB CRUD & Aggregation Pipeline Interface

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://abuawaish-app.streamlit.app/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A **Streamlit**‑based graphical interface for MongoDB that supports full **CRUD** operations, database and collection management, index and schema validation, and **12 predefined aggregation pipelines** (including a join pipeline).  
The backend uses PyMongo and the provided `MongoDbOperation` class; outputs are beautified with the **Rich** library.

---

## Table of Contents

1. [Features](#features)
2. [Prerequisites](#prerequisites)
3. [Setting Up MongoDB Atlas](#setting-up-mongodb-atlas)
   - [Create a Cluster](#create-a-cluster)
   - [Create a Database User](#create-a-database-user)
   - [Configure Network Access](#configure-network-access)
   - [Get Your Connection String](#get-your-connection-string)
4. [Configuration & Installation](#configuration--installation)
   - [Clone or Download the Code](#clone-or-download-the-code)
   - [Install Python Dependencies](#install-python-dependencies)
   - [Update MongoDB Credentials](#update-mongodb-credentials)
5. [Running the Application](#running-the-application)
6. [Using the Interface](#using-the-interface)
   - [Database Operations](#database-operations)
   - [Collection Operations](#collection-operations)
   - [CRUD Operations](#crud-operations)
   - [Index Operations](#index-operations)
   - [Schema Operations](#schema-operations)
   - [Aggregation Pipelines](#aggregation-pipelines)
   - [Seed Sample Data](#seed-sample-data)
7. [Predefined Pipelines](#predefined-pipelines)
8. [Troubleshooting](#troubleshooting)
9. [License](#license)

---

## Features

- **Database management** – list, create, drop databases
- **Collection management** – list, create (with JSON Schema validator), drop, show validation rules
- **Full CRUD** – insert (one / many), fetch all, update (one / many), delete (one / many)
- **Index management** – list indexes, create ascending unique index, drop index
- **Schema validation** – modify validation rules of an existing collection
- **12 predefined aggregation pipelines** – executed on the `Test.cars` or `store_db.users/orders` collections (see below)
- **Sample data seeding** – populate the required collections with the provided car, user and order data
- **Beautiful output** – JSON results are rendered with syntax highlighting; Rich library improves readability in the terminal and in the Streamlit UI

---

## Prerequisites

- Python 3.8 or higher
- A [MongoDB Atlas](https://www.mongodb.com/cloud/atlas) account (free tier works perfectly)
- Basic knowledge of MongoDB (collections, documents, aggregation pipelines)

---

## Setting Up MongoDB Atlas

Follow these steps to create a MongoDB Atlas cluster and obtain your connection string.

### Create a Cluster

1. Log in to [MongoDB Atlas](https://cloud.mongodb.com/).
2. Click **Create** (or **Build a Database**).
3. Choose the **FREE** (M0) tier.
4. Select a cloud provider (AWS, GCP, or Azure) and a region close to you.
5. Give your cluster a name (e.g., `MyCluster`) and click **Create Cluster**.
6. Wait a few minutes for the cluster to be ready.

### Create a Database User

1. In the left sidebar, go to **Database Access**.
2. Click **Add New Database User**.
3. Choose **Password** authentication.
4. Enter a username and a strong password.  
   **Important:** If your password contains special characters like `#`, `@`, `/`, you must URL‑encode them later when building the connection string. The provided code uses `quote_plus` to handle this.
5. Set **Built‑in Role** to `Read and write to any database` (or `Atlas Admin` for full control).
6. Click **Add User**.

### Configure Network Access

1. In the left sidebar, go to **Network Access**.
2. Click **Add IP Address**.
3. For development, you can click **Allow Access from Anywhere** (`0.0.0.0/0`).  
   (For production, restrict to your specific IP.)
4. Click **Confirm**.

### Get Your Connection String

1. In the left sidebar, go to **Database**.
2. Click **Connect** for your cluster.
3. Choose **Connect your application**.
4. Select **Python** and the latest version.
5. Copy the connection string. It looks like:
```text
mongodb+srv://<username>:<password>@<cluster-url>/?retryWrites=true&w=majority
```
6. Replace `<username>` and `<password>` with the credentials you created.  
If your password contains special characters, URL‑encode them (the code does this automatically).

---

## Configuration & Installation

### Clone or Download the Code

Place the following files in the same directory:

- `pymongo_config` – the `MongoDbOperation` class (provided)
- `pymongo_pipelines.py` – Aggregation pipelines and sample data
- `app.py` – the Streamlit interface (code provided in the answer)

### Install Python Dependencies

Create a virtual environment (optional but recommended) and install the required packages:

```bash
pip install streamlit pymongo rich
```
## Update MongoDB Credentials

### The `pymongo_config.py` file currently contains hard‑coded credentials (username your_username and a password).
### You must replace them with your own MongoDB Atlas credentials.

- Open `pymongo_config.py` and locate the `__connect` method:

```python
username: str = "your_username"
password: str = quote_plus("your_password")
uri: str = f"mongodb+srv://{username}:{password}@mycluster.ewovdrg.mongodb.net/?retryWrites=true&w=majority&appName=MyCluster"
```
### Change the username and password to your own.
### If your password contains special characters, quote_plus will encode them automatically – do not pre‑encode them yourself.

- Optional – Use environment variables (more secure):

```python
import os
username = os.getenv("MONGO_USER", "your_username")
password = quote_plus(os.getenv("MONGO_PASS", "your_password"))
```
- Then set MONGO_USER and MONGO_PASS in your shell before running Streamlit.

## Running the Application

- From the terminal, inside the directory containing `app.py`, run:

```python
streamlit run app.py
```
- Streamlit will open a new tab in your default browser. If it doesn’t, you can manually open `http://localhost:8501`.

## Using the Interface
- The left sidebar contains a radio menu with seven categories. Select any category to expand its controls.

## Database Operations

- `List all databases` – shows every database in your Atlas cluster.

- `Create database` – creates a new database (by inserting a dummy collection and removing it).

- `Drop database` – permanently deletes a database.

## Collection Operations

- `List collections` – shows all collections inside a given database.

- `Create collection` – creates a collection; you can optionally provide a JSON Schema validator (e.g., {"$jsonSchema": {"required": ["name"]}}).

- `Drop collection` – deletes a collection.

- `Get collection info` – displays the validation rules (if any) for a collection

## CRUD Operations

- `Insert document(s)` – accepts a JSON object (single document) or a JSON array (multiple documents).

- `Fetch all documents` – retrieves every document from the selected collection and displays them as JSON.

- `Update documents` – specify a filter (JSON) and the update values (JSON). Choose one or many.

- `Delete documents` – specify a filter and whether to delete one or many documents.

## Index Operations

- `Show indexes` – lists all indexes on a collection.

- `Create index` – creates an ascending unique index on a single field (field name is required).

- `Drop index` – deletes an index by its name.

## Schema Operations

- `Modify collection schema` – updates the JSON Schema validator of an existing collection. The new validator must be a valid JSON Schema object.

## Aggregation Pipelines

- Select one of the 12 predefined pipelines from the dropdown.

- Click Execute selected pipeline.

- The pipeline runs on the correct `database/collection` (most on `Test.cars`, the join pipeline on `store_db.users`).

- The JSON result is displayed with syntax highlighting.

## Seed Sample Data

- Before using the aggregation pipelines, you need to insert the sample data:

- Insert cars into `Test.cars` – loads 14 car documents.

- Insert users into `store_db.users` – loads 5 user documents.

- Insert orders into `store_db.orders` – loads 5 order documents.

- These buttons will create the databases and collections automatically if they don’t exist.

## Predefined Pipelines

### 📂 Collection: Test.cars

| #  | Name                                             | Description                                                                 | Target Collection |
|----|--------------------------------------------------|-----------------------------------------------------------------------------|-------------------|
| 1  | Group by fuel_type & count engines >1000cc       | Groups cars by fuel type; counts total and those with engine >1000cc       | Test.cars         |
| 2  | Add is_diesel flag                              | Projects model and a boolean whether fuel_type contains "Dies"             | Test.cars         |
| 3  | Average price per model                         | Computes average price for each model                                      | Test.cars         |
| 4  | Hyundai cars uppercase names                    | Converts maker+model to uppercase, writes to hyundai_cars collection       | Test.cars         |
| 5  | Add 55,000 to price                             | Adds a constant to the price field                                         | Test.cars         |
| 6  | Price in lakhs (string)                         | Converts price to a string like "12.5 lakhs"                               | Test.cars         |
| 7  | Total service cost per Hyundai car              | Sums the service_history.cost for each Hyundai car                         | Test.cars         |
| 8  | Categorise fuel as Petrol_car / Non_petrol_car  | Adds a new field based on fuel_type                                        | Test.cars         |
| 9  | Budget category based on price                  | Adds budget_cat: Budget (<5L), Mid_range (5L-10L), Premium (>10L)           | Test.cars         |
| 10 | Service cost status (High/Low) for Hyundai      | Adds cost_status based on total service cost (≥10000 → High)               | Test.cars         |

---

## 🔗 Join Operation

| Name               | Description                                                        | Target Collection   |
|--------------------|--------------------------------------------------------------------|---------------------|
| Users with orders  | Performs a $lookup from users to orders on user_id                | store_db.users      |

---

## 📝 Notes
- All operations use MongoDB Aggregation Framework  
- Ensure proper indexing for better performance

## Troubleshooting

### Connection failures

- Verify your IP is whitelisted in MongoDB Atlas Network Access.

- Check that the username and password in `pymongo_config.py` are correct.

- If your password contains special characters, make sure you use quote_plus (the code already does).

## Collection or database not found

- Use the Create database / Create collection buttons first, or use the Seed Sample Data buttons which create them automatically.

## Pipeline execution produces no output

- Ensure the target collection contains data (run the seed buttons).

- Some pipelines (e.g., `pipeline_4`) write results to a new collection – check the `hyundai_cars` collection.

## Rich library output not showing in Streamlit

- The app falls back to plain text if Rich rendering fails. Make sure you have installed rich (`pip install rich`).

## License
- This project is provided for educational purposes. You are free to modify and use it as needed.

```text
This README covers everything: what the app does, how to set up MongoDB Atlas, how to configure credentials, installation, running, and detailed usage. The user can simply copy‑paste this into a `README.md` file in the project root.
```