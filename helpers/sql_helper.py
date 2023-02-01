import sqlite3


class SqlHelper:
    """
    Helper class to push data into a locally running Sql Lite DB
    """
    def __init__(self):
        self.conn = sqlite3.connect('db.sqlite')
        self.cursor = self.conn.cursor()

    def to_db(self, output_df, table_name, column_ddl):
        # conn = sqlite3.connect('db.sqlite')
        cursor = self.conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS {0}({1})".format(table_name,column_ddl))
        self.conn.commit()
        output_df.to_sql(table_name, "sqlite:////Users/a.shetty/db.sqlite", if_exists='replace', index= False)