import configparser
import psycopg2
from sql_queries import analytics_queries


def analytics(cur, conn):
    """
    Run querries writen in the SQL script 
    """
    for query in analytics_queries:
        print(query)
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn= psycopg2.connect('host={} dbname={} user={} password={} port={}'.format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    analytics(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()