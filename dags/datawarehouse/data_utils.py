from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyscope2.extras import RealDictCursor


table = "yt_api"
def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursorfor_factory=RealDictCursor)
    return conn, cur
    
def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()
def create_schema(schema):
    conn, cur = get_conn_cursor()
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
    cur.execute(sql)
    conn.commit()
    close_conn_cursor(conn, cur)
    
def create_table(schema):
    conn, cur = get_conn_cursor()
    
    if schema == "staging":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "video_id" VARCHAR(11) PRIMARY KEY NOT NULL,
                "video_title" TEXT NOT NULL,
                "upload_date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "video_views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT,
            );
        """
            
        
    
    
    