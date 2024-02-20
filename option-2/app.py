import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark.exceptions

session = get_active_session()

def get_logs(query, message):
    logging_query = f"""
    INSERT INTO STREAMLIT_DB.SIS.SIS_LOGGING (TS, DB_NAME, SCH_NAME, STREAMLIT_APP, USER_NAME, QUERY_TEXT, LOG_MSG, WAREHOUSE_NAME)
    VALUES
        (CURRENT_TIMESTAMP(), '{session.get_current_database().replace('"', "")}', '{session.get_current_schema().replace('"', "")}', 'SIS_EXAMPLE_APP','{st.experimental_user["user_name"]}', '{query}', '{message}', '{session.get_current_warehouse().replace('"', "")}')
    """
    st.write(logging_query)

    try:
        logging_data = session.sql(logging_query).collect()
        st.success("Data was written to the SIS_LOGGING table.")
    except:
        st.error("Data was not written to the SIS_LOGGING table.")

def output_error(query, message):
    get_logs(query, message)
    st.error(message)

def log_query(query):
    try:
        query_execution = session.sql(query).collect()
        message = "Success"
        get_logs(query, message)
        st.dataframe(query_execution)
    except snowflake.snowpark.exceptions.SnowparkColumnException as SnowparkColumnException:
        message = str(SnowparkColumnException).replace("'", "")
        message = f"SnowparkColumnException: {message}"
        output_error(query, message)
    except snowflake.snowpark.exceptions.SnowparkCreateViewException as SnowparkCreateViewException:
        message = str(SnowparkCreateViewException).replace("'", "")
        message = f"SnowparkCreateViewException: {message}"
        output_error(query, message)
    except snowflake.snowpark.exceptions.SnowparkDataframeException as SnowparkDataframeException:
        message = str(SnowparkDataframeException).replace("'", "")
        message = f"SnowparkDataframeException: {message}"
        output_error(query, message)
    except snowflake.snowpark.exceptions.SnowparkFetchDataException as SnowparkFetchDataException:
        message = str(SnowparkFetchDataException).replace("'", "")
        message = f"SnowparkFetchDataException: {message}"
        output_error(query, message)
    except snowflake.snowpark.exceptions.SnowparkGeneralException as SnowparkGeneralException:
        message = str(SnowparkGeneralException).replace("'", "")
        message = f"SnowparkGeneralException: {message}"
        output_error(query, message)
    except snowflake.snowpark.exceptions.SnowparkMissingDbOrSchemaException as SnowparkMissingDbOrSchemaException:
        message = str(SnowparkMissingDbOrSchemaException).replace("'", "")
        message = f"SnowparkMissingDbOrSchemaException: {message}"
        output_error(query, message)
    except snowflake.snowpark.exceptions.SnowparkPlanException as SnowparkPlanException:
        message = str(SnowparkPlanException).replace("'", "")
        message = f"SnowparkPlanException: {message}"
        output_error(query, message)
    except Exception as e:
        message = str(e).replace("'", "").lstrip()
        output_error(query, message)

def main():
    st.title("Example Streamlit App")

    query_text = "SELECT * FROM STREAMLIT_DB.SIS.SIS_LOGGING"
    log_query(query_text)

    query_text_2 = "SELECT * FROM TEST_DB"
    log_query(query_text_2)

main()
