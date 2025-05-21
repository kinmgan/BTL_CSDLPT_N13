import testHelper
import Interface as MyAssignment  # Phải đặt tên y chang như trong Assignment1Tester.py

DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE = 'ratings'
INPUT_FILE_PATH = 'test_data.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 20  # file test_data.dat có 20 dòng

if __name__ == '__main__':
    try:
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(0)
            testHelper.deleteAllPublicTables(conn)

            [result, e] = testHelper.testloadratings(
                MyAssignment,
                RATINGS_TABLE,
                INPUT_FILE_PATH,
                conn,
                ACTUAL_ROWS_IN_INPUT_FILE
            )

            if result:
                print("✅ Hàm loadratings() PASS.")
            else:
                print("❌ Hàm loadratings() FAIL.")
                print(e)

    except Exception as ex:
        print("❌ Lỗi khi test:")
        print(ex)
