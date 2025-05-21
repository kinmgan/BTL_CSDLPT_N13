import psycopg2

def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host='localhost'
    )

def loadratings(ratingstablename, filepath, openconnection):
    cur = openconnection.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INT,
            movieid INT,
            rating FLOAT
        );
    """)
    openconnection.commit()

    with open(filepath, 'r') as file:
        for line in file:
            parts = line.strip().split("::")
            if len(parts) == 4:
                userid, movieid, rating = int(parts[0]), int(parts[1]), float(parts[2])
                cur.execute(f"""
                    INSERT INTO {ratingstablename} (userid, movieid, rating)
                    VALUES (%s, %s, %s)
                """, (userid, movieid, rating))

    openconnection.commit()
    cur.close()
