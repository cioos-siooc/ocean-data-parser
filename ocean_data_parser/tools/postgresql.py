from io import StringIO
import csv
import logging

logger = logging.getLogger(__name__)


def update_database_table(
    df, table, conn, distinct_columns=None, schema=None, if_row_exist="UPDATE"
):
    """
    Method use to update database table, it first upload to
    a temporary table, which then update the original table with any new sample that aren't available already.
    """

    def psql_insert_copy(table, conn, keys, data_iter):
        """
        Execute SQL statement inserting data into a postgresql db with using COPY from CSV to a temporary table and then update on conflict or nothing

        Parameters
        ----------
        table : pandas.io.sql.SQLTable
        conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys : list of str
            Column names
        data_iter : Iterable that iterates the values to be inserted
        """
        # gets a DBAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ", ".join(f'"{k}"' for k in keys)
            table_name = f"{table.schema}.{table.name}" if table.schema else table.name
            if distinct_columns:
                on_conflict = f"ON CONFLICT ({','.join(distinct_columns)}) DO "
                if if_row_exist == "UPDATE":
                    on_conflict += f"UPDATE  SET ({','.join(available_columns)}) = ({','.join([f'EXCLUDED.{item}' for item in available_columns])})"

                else:
                    on_conflict += "NOTHING"
            else:
                on_conflict = ""

            sql = f"""
            CREATE TEMP TABLE tmp_table
            (LIKE {table_name} INCLUDING DEFAULTS)
            ON COMMIT DROP;

            COPY tmp_table ({columns}) FROM STDIN WITH CSV;
        
            INSERT INTO {table_name}
            SELECT * FROM tmp_table
            {on_conflict};
            """
            cur.copy_expert(sql=sql, file=s_buf)

    # Sort columns to be same as datbase ignore the extra variables
    table_columns = list(
        conn.execute(f"SELECT * FROM {schema+'.' or ''}{table}").keys()
    )
    available_columns = [
        col for col in table_columns if col in df or col in df.index.names
    ]
    df_update = df.reset_index()[available_columns]

    logging.info(f"Append data to table {table}")
    df_update.to_sql(
        table,
        schema=schema,
        if_exists="append",
        con=conn,
        index=False,
        method=psql_insert_copy,
    )


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join(f'"{k}"' for k in keys)
        table_name = f"{table.schema}.{table.name}" if table.schema else table.name
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)
