from io import StringIO
import  psycopg2
from psycopg2 import sql, extensions
DATABASE_NAME        = 'dds_assgn1'
RANGE_TABLE_PREFIX   = 'range_part'  
RROBIN_TABLE_PREFIX  = 'rrobin_part'
USER_ID_COLNAME      = 'userid'
MOVIE_ID_COLNAME     = 'movieid'
RATING_COLNAME       = 'rating'
def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host='localhost'
    )

def create_db(dbname):
    """
    Tạo cơ sở dữ liệu nếu chưa tồn tại, kết nối đến mặc định 'postgres'
    """
    con = getopenconnection(dbname='postgres')  
    con.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute(
        "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s;",
        (dbname,)
    )
    exists = cur.fetchone()
    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        print(f"Database '{dbname}' created.")
    else:
        print(f"Database '{dbname}' already exists.")

    cur.close()
    con.close()

def loadratings(ratingstablename, filepath, openconnection):
    create_db(DATABASE_NAME)
    cur = openconnection.cursor()

    cur.execute("SET synchronous_commit TO OFF;")
    cur.execute("SET temp_buffers TO '64MB';")
    cur.execute("SET work_mem TO '64MB';")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INT,
            movieid INT,
            rating FLOAT
        );
    """)

    buffer = StringIO()
    with open(filepath, 'r') as file:
        buffer.writelines(
            f"{line.split('::', 3)[0]},{line.split('::', 3)[1]},{line.split('::', 3)[2]}\n"
            for line in file
        )

    buffer.seek(0)

    cur.copy_expert(f"""
        COPY {ratingstablename}(userid, movieid, rating)
        FROM STDIN WITH (FORMAT csv)
    """, buffer)

    openconnection.commit()
    cur.close()
    
def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    cur.execute(f"SELECT tablename FROM pg_tables WHERE tablename LIKE '{RANGE_TABLE_PREFIX}%'")
    drop_tables = [row[0] for row in cur.fetchall()]
    if drop_tables:
        cur.execute("DROP TABLE IF EXISTS " + ", ".join(drop_tables))
    min_rating = 0.0
    max_rating = 5.0
    delta = float(max_rating - min_rating) / numberofpartitions
    for i in range(numberofpartitions):
        lower = min_rating + i * delta
        upper = lower + delta
        part_table = f"{RANGE_TABLE_PREFIX}{i}"
        if i == 0:
            where_clause = f"rating >= {lower} AND rating <= {upper}"
        else:
            where_clause = f"rating > {lower} AND rating <= {upper}"
        cur.execute(
            f"CREATE TABLE {part_table} AS "
            f"SELECT userid, movieid, rating FROM {ratingstablename} WHERE {where_clause}"
        )
    cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute(
        f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
        (userid, itemid, rating)
    )
    cur.execute("SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE 'range_part%'")
    numberofpartitions = cur.fetchone()[0]
    min_rating = 0.0
    max_rating = 5.0
    delta = (max_rating - min_rating) / numberofpartitions
    if rating <= min_rating + delta:
        index = 0
    else:
        index = int((rating - min_rating - 1e-8) // delta)
        if index >= numberofpartitions:
            index = numberofpartitions - 1
    part_table = f"range_part{index}"
    cur.execute(
        f"INSERT INTO {part_table} (userid, movieid, rating) VALUES (%s, %s, %s)",
        (userid, itemid, rating)
    )
    cur.close()

def roundrobinpartition(ratingstablename: str, numberofpartitions: int, openconnection):
    if numberofpartitions <= 0:
        raise ValueError('numberofpartitions phải > 0')

    cur = openconnection.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name LIKE %s;",
        (f'{RROBIN_TABLE_PREFIX}%',)
    )
    for tbl, in cur.fetchall():
        cur.execute(f'DROP TABLE IF EXISTS "{tbl}" CASCADE;')
    cur.execute("SET synchronous_commit TO OFF;")
    for i in range(numberofpartitions):
        cur.execute(f"""
            CREATE UNLOGGED TABLE {RROBIN_TABLE_PREFIX}{i} (
                {USER_ID_COLNAME}  INT,
                {MOVIE_ID_COLNAME} INT,
                {RATING_COLNAME}   FLOAT
            );
        """)
    cur.execute(f"""
        CREATE TEMPORARY TABLE numbered AS
        SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME},
               ROW_NUMBER() OVER () - 1 AS rn
        FROM {ratingstablename};
    """)
    for i in range(numberofpartitions):
        cur.execute(f"""
            INSERT INTO {RROBIN_TABLE_PREFIX}{i}
            SELECT {USER_ID_COLNAME}, {MOVIE_ID_COLNAME}, {RATING_COLNAME}
            FROM numbered
            WHERE (rn % {numberofpartitions}::BIGINT) = {i};
        """)
    cur.execute("DROP TABLE numbered;")
    openconnection.commit()
    cur.close()

def roundrobininsert(ratingstablename: str,
                     userid: int, movieid: int, rating: float,
                     openconnection):

    cur = openconnection.cursor()

    cur.execute(f"""
        INSERT INTO {ratingstablename} ({USER_ID_COLNAME},
                                         {MOVIE_ID_COLNAME},
                                         {RATING_COLNAME})
        VALUES (%s, %s, %s);
    """, (userid, movieid, rating))
    cur.execute("""
        SELECT COUNT(*)
        FROM   information_schema.tables
        WHERE  table_schema='public'
          AND  table_name LIKE %s;
    """, (f'{RROBIN_TABLE_PREFIX}%',))
    n = cur.fetchone()[0]
    if n == 0:
        raise RuntimeError('Phải gọi roundrobinpartition trước!')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rrobin_meta (
            dummy     INT PRIMARY KEY DEFAULT 1,
            next_part INT
        );
    """)
    cur.execute("SELECT next_part FROM rrobin_meta FOR UPDATE;")
    row = cur.fetchone()
    next_part = 0 if row is None else row[0]
    partition_tbl = f'{RROBIN_TABLE_PREFIX}{next_part}'
    cur.execute(f"""
        INSERT INTO {partition_tbl} ({USER_ID_COLNAME},
                                     {MOVIE_ID_COLNAME},
                                     {RATING_COLNAME})
        VALUES (%s, %s, %s);
    """, (userid, movieid, rating))
    cur.execute("""
        INSERT INTO rrobin_meta (next_part)
        VALUES (%s)
        ON CONFLICT (dummy)
        DO UPDATE SET next_part = (%s + 1) %% %s;
    """, ( (next_part + 1) % n, next_part, n ))

    openconnection.commit()
    cur.close()

