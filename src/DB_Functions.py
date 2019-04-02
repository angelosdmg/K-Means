import psycopg2
from config import *


def truncate_hubspot_deal_stages_table():
    truncate_table_gold('hubspot_stages')

def truncate_hubspot_owners_table():
    truncate_table_gold('hubspot_owners')

def truncate_hubspot_engagements_table():
    truncate_table_gold('hubspot_engagements')

def truncate_hubspot_deals_table():
    truncate_table_gold('hubspot_deals')



def truncate_table_gold(table):
    """
    Truncates a table stored in GOLD DB

    @type table: str
    @param table: The name of the table

    """

    print('truncating table ' + table)

    sql = """TRUNCATE TABLE """+ table
    execute_general_sql_command_gold(sql)

    print('truncation complete')



def execute_general_sql_command_gold(sql):
    """
    Executes Insertions and Updates in GOLD

    @type sql: str
    @param sql: SQL Command to be executed
    """

    conn = None
    try:
        params = config_gold()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)

        conn.commit()

        cur.close

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

# TODO: Print the result of commit function in order to validate results
def import_df_to_gold_db(table_name, ifexists, df):
    """
    Imports a DataDrame in Gold DB.

    Important: When 'ifexists = append', the order of the columns in the DataFrame should follow the order of the
    columns in the table stored in database. For this purpose, It is advisable to specify the order of the columns
    in the dataframe before using this function

    @type table_name: str
    @param table_name: Table to insert values to
    @type ifexists: str
    @param ifexists: defines the insert operation in to_sql function. There are three options: {fail, replce, append}
    @type df: dataFrame
    @param df: DataFrame containing the respective values
    """
    print('import dataframe to database')
    #ifexists is usually 'append'
    from sqlalchemy import create_engine
    import psycopg2
    import io

    engine = create_engine('postgresql+psycopg2://u8f7e2s1opi77m:p446b244b3e44cdcae1fd3ca10c1d073c2faaf8a9b8674a50f0ebfe381baafa29@ec2-34-232-104-183.compute-1.amazonaws.com:5432/d106maq19do6j4')

    df.head(0).to_sql(table_name, engine, if_exists=ifexists,index=False) #truncates the table

    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, table_name, null="") # null values become ''
    conn.commit()
    print('import finished')
