import psycopg2

conn = psycopg2.connect(dbname="postgres")

print(conn.result())