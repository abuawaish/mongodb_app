import streamlit as st
import json
import sys
import io
import contextlib
from typing import Any, Dict, List, Optional
from rich.console import Console
from rich.table import Table
from rich.json import JSON as RichJSON

# Import the provided MongoDB operations and pipelines
from pymongo_config import MongoDbOperation
from pymongo_pipelines import Pipelines

# ----------------------------------------------------------------------
# Helper: capture stdout/stderr of any function and return as string
# ----------------------------------------------------------------------
def capture_output(func, *args, **kwargs) -> str:
    """Call func(*args, **kwargs) and return everything printed to stdout/stderr."""
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    with contextlib.redirect_stdout(stdout_capture), contextlib.redirect_stderr(stderr_capture):
        try:
            func(*args, **kwargs)
        except Exception as e:
            # If the function raises, we still want to show the exception
            print(f"ERROR: {e}")
    return stdout_capture.getvalue() + stderr_capture.getvalue()


# ----------------------------------------------------------------------
# Helper: display captured text or Rich output
# ----------------------------------------------------------------------
def display_output(output_text: str, title: str = "Output", use_rich: bool = True):
    """Show output in Streamlit, optionally using Rich formatting."""
    if not output_text.strip():
        st.info("No output generated.")
        return
    if use_rich:
        # Try to render as Rich JSON if it looks like JSON
        try:
            parsed = json.loads(output_text)
            rich_json = RichJSON(parsed)
            console = Console(file=io.StringIO(), force_terminal=False)
            console.print(rich_json)
            formatted = console.file.getvalue()
            st.code(formatted, language="json")
        except:
            # Fallback: plain text in code block
            st.code(output_text, language="text")
    else:
        st.text_area(title, output_text, height=300)


# ----------------------------------------------------------------------
# Helper: parse JSON from text area
# ----------------------------------------------------------------------
def parse_json(json_str: str, default: Any = None) -> Any:
    if not json_str.strip():
        return default
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON: {e}")
        return None


# ----------------------------------------------------------------------
# Streamlit App
# ----------------------------------------------------------------------
st.set_page_config(page_title="MongoDB CRUD + Pipelines", layout="wide")
st.title("🍃 MongoDB CRUD & Aggregation Pipeline Interface")
st.markdown("Perform database operations and execute predefined aggregation pipelines.")

# Sidebar navigation
menu = st.sidebar.radio(
    "Choose operation category",
    [
        "Database Ops",
        "Collection Ops",
        "CRUD Ops",
        "Index Ops",
        "Schema Ops",
        "Aggregation Pipelines",
        "Seed Sample Data",
    ],
)

# ----------------------------------------------------------------------
# 1. Database Operations
# ----------------------------------------------------------------------
if menu == "Database Ops":
    st.header("📀 Database Operations")

    if st.button("List all databases"):
        output = capture_output(MongoDbOperation.get_database_names)
        display_output(output, "Databases")

    with st.expander("Create database"):
        db_name = st.text_input("Database name", key="create_db")
        if st.button("Create database"):
            if db_name:
                output = capture_output(MongoDbOperation.create_database, db_name)
                display_output(output, "Creation result")
            else:
                st.warning("Enter a database name.")

    with st.expander("Drop database"):
        db_name = st.text_input("Database name", key="drop_db")
        if st.button("Drop database"):
            if db_name:
                output = capture_output(MongoDbOperation.drop_database, db_name)
                display_output(output, "Drop result")
            else:
                st.warning("Enter a database name.")

# ----------------------------------------------------------------------
# 2. Collection Operations
# ----------------------------------------------------------------------
elif menu == "Collection Ops":
    st.header("📁 Collection Operations")

    col1, col2 = st.columns(2)
    with col1:
        db_name = st.text_input("Database name", key="coll_db")
        if st.button("List collections"):
            if db_name:
                output = capture_output(MongoDbOperation.get_collection_names, db_name)
                display_output(output, f"Collections in {db_name}")
            else:
                st.warning("Enter database name.")

    with col2:
        db_name = st.text_input("Database name", key="coll_create_db")
        coll_name = st.text_input("Collection name", key="coll_create")
        validator_json = st.text_area(
            "Validator (JSON Schema) – optional",
            value="{}",
            height=150,
            help='Example: {"$jsonSchema": {"required": ["name"]}}',
        )
        if st.button("Create collection"):
            if db_name and coll_name:
                validator = parse_json(validator_json, None)
                output = capture_output(
                    MongoDbOperation.create_collection, db_name, coll_name, validator
                )
                display_output(output, "Creation result")
            else:
                st.warning("Database and collection names required.")

    with st.expander("Drop collection"):
        db_name = st.text_input("Database name", key="drop_coll_db")
        coll_name = st.text_input("Collection name", key="drop_coll")
        if st.button("Drop collection"):
            if db_name and coll_name:
                output = capture_output(MongoDbOperation.drop_collection, db_name, coll_name)
                display_output(output, "Drop result")
            else:
                st.warning("Both names required.")

    with st.expander("Show collection info (validator)"):
        db_name = st.text_input("Database name", key="info_db")
        coll_name = st.text_input("Collection name", key="info_coll")
        if st.button("Get collection info"):
            if db_name and coll_name:
                output = capture_output(MongoDbOperation.get_collection_info, db_name, coll_name)
                display_output(output, f"Info for {coll_name}")
            else:
                st.warning("Both names required.")

# ----------------------------------------------------------------------
# 3. CRUD Operations
# ----------------------------------------------------------------------
elif menu == "CRUD Ops":
    st.header("✏️ CRUD Operations")
    db_name = st.text_input("Database name", key="crud_db")
    coll_name = st.text_input("Collection name", key="crud_coll")

    # Insert
    with st.expander("Insert document(s)"):
        doc_json = st.text_area(
            "Document(s) (JSON object or array)",
            value='{"name": "example", "value": 123}',
            height=150,
        )
        if st.button("Insert"):
            if db_name and coll_name and doc_json:
                doc = parse_json(doc_json)
                if doc:
                    output = capture_output(MongoDbOperation.insert_document, db_name, coll_name, doc)
                    display_output(output, "Insert result")
            else:
                st.warning("All fields required.")

    # Fetch
    if st.button("Fetch all documents"):
        if db_name and coll_name:
            output = capture_output(MongoDbOperation.fetch_document, db_name, coll_name)
            display_output(output, f"Documents from {coll_name}")
        else:
            st.warning("Database and collection names required.")

    # Update
    with st.expander("Update documents"):
        filter_json = st.text_area("Filter (JSON)", value="{}", height=100)
        update_json = st.text_area("Update values (JSON)", value='{"field": "new_value"}', height=100)
        update_type = st.radio("Update type", ("one", "many"), horizontal=True)
        if st.button("Update"):
            if db_name and coll_name and filter_json and update_json:
                filter_doc = parse_json(filter_json)
                update_doc = parse_json(update_json)
                if filter_doc is not None and update_doc is not None:
                    output = capture_output(
                        MongoDbOperation.update_document,
                        db_name,
                        coll_name,
                        filter_doc,
                        update_doc,
                        update_type,
                    )
                    display_output(output, "Update result")
            else:
                st.warning("All fields required.")

    # Delete
    with st.expander("Delete documents"):
        filter_json = st.text_area("Delete filter (JSON)", value="{}", height=100)
        delete_type = st.radio("Delete type", ("one", "many"), horizontal=True)
        if st.button("Delete"):
            if db_name and coll_name and filter_json:
                filter_doc = parse_json(filter_json)
                if filter_doc is not None:
                    output = capture_output(
                        MongoDbOperation.delete_document,
                        db_name,
                        coll_name,
                        filter_doc,
                        delete_type,
                    )
                    display_output(output, "Delete result")
            else:
                st.warning("All fields required.")

# ----------------------------------------------------------------------
# 4. Index Operations
# ----------------------------------------------------------------------
elif menu == "Index Ops":
    st.header("🔍 Index Management")
    db_name = st.text_input("Database name", key="idx_db")
    coll_name = st.text_input("Collection name", key="idx_coll")

    if st.button("Show indexes"):
        if db_name and coll_name:
            output = capture_output(MongoDbOperation.show_indexes, db_name, coll_name)
            display_output(output, f"Indexes on {coll_name}")
        else:
            st.warning("Both names required.")

    with st.expander("Create index"):
        field_name = st.text_input("Field name for index", key="idx_field")
        if st.button("Create index"):
            if db_name and coll_name and field_name:
                output = capture_output(MongoDbOperation.create_index, db_name, coll_name, field_name)
                display_output(output, "Index creation result")
            else:
                st.warning("All fields required.")

    with st.expander("Drop index"):
        index_name = st.text_input("Index name to drop", key="drop_idx")
        if st.button("Drop index"):
            if db_name and coll_name and index_name:
                output = capture_output(MongoDbOperation.drop_index, db_name, coll_name, index_name)
                display_output(output, "Drop index result")
            else:
                st.warning("All fields required.")

# ----------------------------------------------------------------------
# 5. Schema Operations (modify validator)
# ----------------------------------------------------------------------
elif menu == "Schema Ops":
    st.header("📐 Schema Validation")
    db_name = st.text_input("Database name", key="schema_db")
    coll_name = st.text_input("Collection name", key="schema_coll")
    validator_json = st.text_area(
        "New validator (JSON Schema)",
        value='{"$jsonSchema": {"required": ["name"]}}',
        height=200,
    )
    if st.button("Modify collection schema"):
        if db_name and coll_name and validator_json:
            validator = parse_json(validator_json)
            if validator:
                output = capture_output(
                    MongoDbOperation.modify_existing_collection_schema,
                    db_name,
                    coll_name,
                    validator,
                )
                display_output(output, "Schema modification result")
        else:
            st.warning("All fields required.")

# ----------------------------------------------------------------------
# 6. Aggregation Pipelines (predefined)
# ----------------------------------------------------------------------
elif menu == "Aggregation Pipelines":
    st.header("⚙️ Execute Predefined Pipelines")

    # List of available pipelines from Pipelines class
    pipeline_map = {
        "1. Group by fuel_type & count engines >1000cc": Pipelines.pipeline_1,
        "2. Add is_diesel flag": Pipelines.pipeline_2,
        "3. Average price per model": Pipelines.pipeline_3,
        "4. Hyundai cars uppercase names (writes to hyundai_cars)": Pipelines.pipeline_4,
        "5. Add 55,000 to price": Pipelines.pipeline_5,
        "6. Price in lakhs (string)": Pipelines.pipeline_6,
        "7. Total service cost per Hyundai car": Pipelines.pipeline_7,
        "8. Categorise fuel as Petrol_car / Non_petrol_car": Pipelines.pipeline_8,
        "9. Budget category based on price": Pipelines.pipeline_9,
        "10. Service cost status (High/Low) for Hyundai": Pipelines.pipeline_10,
        "Join: Users with orders (store_db)": Pipelines.join_pipeline,
    }

    selected = st.selectbox("Choose a pipeline", list(pipeline_map.keys()))
    if st.button("Execute selected pipeline"):
        pipeline_func = pipeline_map[selected]
        pipeline = pipeline_func()

        # Decide which method to call
        if "Join" in selected:
            # join pipeline works on store_db.users
            output = capture_output(MongoDbOperation.aggregate_join_collection, pipeline)
        else:
            # all other pipelines work on Test.cars
            output = capture_output(MongoDbOperation.execute_aggregate_pipeline, pipeline)

        st.subheader("Pipeline Result")
        # Try to pretty-print the captured JSON with Rich
        try:
            # The output is JSON printed by the original method
            data = json.loads(output)
            rich_json = RichJSON(data)
            console = Console(file=io.StringIO(), force_terminal=False)
            console.print(rich_json)
            formatted = console.file.getvalue()
            st.code(formatted, language="json")
        except:
            # If not valid JSON, show raw output
            st.code(output, language="text")

# ----------------------------------------------------------------------
# 7. Seed Sample Data
# ----------------------------------------------------------------------
elif menu == "Seed Sample Data":
    st.header("🌱 Insert Sample Data for Pipelines")
    st.markdown(
        "The predefined pipelines work on the `Test.cars` and `store_db.users/orders` collections. "
        "Use the buttons below to populate them with the sample data from `Pipelines`."
    )

    if st.button("Insert cars into Test.cars"):
        cars = Pipelines.get_cars_data()
        # Insert many
        output = capture_output(
            MongoDbOperation.insert_document, "Test", "cars", cars
        )
        display_output(output, "Cars inserted")

    if st.button("Insert users into store_db.users"):
        users = Pipelines.get_users_data()
        output = capture_output(
            MongoDbOperation.insert_document, "store_db", "users", users
        )
        display_output(output, "Users inserted")

    if st.button("Insert orders into store_db.orders"):
        orders = Pipelines.get_orders_data()
        output = capture_output(
            MongoDbOperation.insert_document, "store_db", "orders", orders
        )
        display_output(output, "Orders inserted")

    st.info(
        "Note: If collections do not exist, they will be created automatically on first insert. "
        "The join pipeline expects both `users` and `orders` collections."
    )