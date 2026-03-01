import psycopg2

try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        database="postgres",
        user="postgres",
        password="N4giev123!!!"
    )
    cur = conn.cursor()
    cur.execute("SELECT current_user, current_database();")
    print(cur.fetchone())
    conn.close()
except Exception as e:
    print("Ошибка подключения:", e)