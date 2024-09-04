from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Database connection
def create_connection():
    engine = create_engine("mysql+mysqlconnector://admin:Gpohealth!#!@dev-db-test.c969yoyq9cyy.us-east-1.rds.amazonaws.com/stg_tbl")
    Session = sessionmaker(bind=engine)
    return Session()

# Fetch all SQL queries ordered by sequence number
def fetch_all_sql_queries(session):
    result = session.execute(text("SELECT ctxval FROM stg_tbl.Query_metadata_context_l2_python where ctxarea='L2_Invoice' order by ctxkey"))
    return result.fetchall()

# Execute the SQL query with optional variables
def execute_sql(session, sql_query, variables=None):
    query = text(sql_query)
    session.execute(query, variables or {})
    session.commit()
    return None

# Main execution
def main():
    # Example: Dictionary of possible variables (adjust per your specific queries)
    variables = {
        "context_customer":"Duke University Health System",
        "context_l1_date":"2022-06-30",
        # Add more variables as needed, or leave some out if not required
    }

    # Create database session
    session = create_connection()

    # Fetch all SQL queries in the correct order
    sql_queries = fetch_all_sql_queries(session)
    print(sql_queries)
    # Execute each SQL query in sequence
    for sql_row in sql_queries:
        sql_query = sql_row[0]

        # Determine which variables are needed by this query
        required_variables = {key: variables[key] for key in variables if f":{key}" in sql_query}
        print(required_variables)

        # Execute the SQL query
        execute_sql(session, sql_query, required_variables)
        print(f"Executed SQL: {sql_query}")
        # for row in results:
        #     print(row)

    # Close the session
    session.close()

if __name__ == "__main__":
    main()
