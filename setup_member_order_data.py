import csv
import random
import datetime
import mysql.connector
import os

# ========================
# 환경 설정
# ========================
MEMBERS = 100_000  # 회원 수
ORDERS = 10_000_000  # 주문 수
BATCH_SIZE = 1_000_000  # CSV 작성시 배치 크기

MEMBERS_CSV = "members.csv"
ORDERS_SINCE_1_YEAR_AGO_CSV = "orders.csv"
ORDERS_SINCE_2019_CSV = "orders_from_2019.csv"


# ========================
# 1. 회원 데이터 생성
# ========================
def generate_members():
    with open(MEMBERS_CSV, mode="w", newline="") as f:
        writer = csv.writer(f)
        for i in range(1, MEMBERS + 1):
            name = f"User{i}"
            email = f"user{i}@test.com"
            created_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([i, name, email, created_at])
            if i % 10_000 == 0:
                print(f"{i:,} members generated...")
    print(f"✅ 회원 CSV 생성 완료: {MEMBERS_CSV}")


# ========================
# 2. 주문 데이터 생성
# ========================
def generate_orders_since_2019():
    start_date = datetime.datetime(2019, 1, 1)
    end_date = datetime.datetime.now()
    total_seconds = int((end_date - start_date).total_seconds())

    with open(ORDERS_SINCE_2019_CSV, mode="w", newline="") as f:
        writer = csv.writer(f)
        for i in range(0, ORDERS, BATCH_SIZE):
            batch = []
            for _ in range(BATCH_SIZE):
                member_id = random.randint(1, MEMBERS)
                amount = round(random.uniform(1, 1000), 2)

                # 2019-01-01 ~ 현재까지 균등 분포로 랜덤 날짜 생성
                rand_seconds = random.randint(0, total_seconds)
                order_date = (start_date + datetime.timedelta(seconds=rand_seconds)).strftime("%Y-%m-%d %H:%M:%S")

                batch.append([member_id, amount, order_date])
            writer.writerows(batch)
            print(f"{i + BATCH_SIZE:,} orders generated...")

    print(f"✅ 주문 CSV 생성 완료: {ORDERS_SINCE_1_YEAR_AGO_CSV}")


def generate_orders_since_1_year_ago():
    with open(ORDERS_SINCE_1_YEAR_AGO_CSV, mode="w", newline="") as f:
        writer = csv.writer(f)
        for i in range(0, ORDERS, BATCH_SIZE):
            batch = []
            for _ in range(BATCH_SIZE):
                member_id = random.randint(1, MEMBERS)
                amount = round(random.uniform(1, 1000), 2)
                days_ago = random.randint(0, 365)
                order_date = (datetime.datetime.now() - datetime.timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
                batch.append([member_id, amount, order_date])
            writer.writerows(batch)
            print(f"{i + BATCH_SIZE:,} orders generated...")
    print(f"✅ 주문 CSV 생성 완료: {ORDERS_SINCE_1_YEAR_AGO_CSV}")


# ========================
# 3. MySQL 적재
# ========================
def load_orders_since_2019():
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="rootpass",
        database="perf_test",
        allow_local_infile=True
    )
    cur = conn.cursor()

    # orders 테이블 적재 (100만건 단위)
    batch_size = 1_000_000
    temp_file = "orders_batch_2019.csv"

    with open(ORDERS_SINCE_2019_CSV, "r") as infile:
        batch = []
        for idx, line in enumerate(infile, start=1):
            batch.append(line)
            if idx % batch_size == 0:
                # 임시 파일에 저장
                with open(temp_file, "w") as f:
                    f.writelines(batch)

                query_orders = f"""
                LOAD DATA LOCAL INFILE '{os.path.relpath(temp_file)}'
                INTO TABLE orders
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY '\\n'
                (member_id, amount, order_date);
                """
                cur.execute(query_orders)
                conn.commit()
                print(f"✅ {idx:,} rows committed")
                batch = []

        # 남은 데이터 처리
        if batch:
            with open(temp_file, "w") as f:
                f.writelines(batch)
            cur.execute(f"""
                LOAD DATA LOCAL INFILE '{os.path.relpath(temp_file)}'
                INTO TABLE orders
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY '\\n'
                (member_id, amount, order_date);
            """)
            conn.commit()
            print(f"✅ {idx:,} rows committed (last batch)")

    cur.close()
    conn.close()
    os.remove(temp_file)
    print("✅ 모든 데이터 적재 완료")


def load_orders_since_1_year_ago():
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="rootpass",
        database="perf_test",
        allow_local_infile=True
    )
    cur = conn.cursor()

    # orders 테이블 적재 (100만건 단위)
    batch_size = 1_000_000
    temp_file = "orders_batch.csv"

    with open(ORDERS_SINCE_1_YEAR_AGO_CSV, "r") as infile:
        batch = []
        for idx, line in enumerate(infile, start=1):
            batch.append(line)
            if idx % batch_size == 0:
                # 임시 파일에 저장
                with open(temp_file, "w") as f:
                    f.writelines(batch)

                query_orders = f"""
                LOAD DATA LOCAL INFILE '{os.path.relpath(temp_file)}'
                INTO TABLE orders_narrow
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY '\\n'
                (member_id, amount, order_date);
                """
                cur.execute(query_orders)
                conn.commit()
                print(f"✅ {idx:,} rows committed")
                batch = []

        # 남은 데이터 처리
        if batch:
            with open(temp_file, "w") as f:
                f.writelines(batch)
            cur.execute(f"""
                LOAD DATA LOCAL INFILE '{os.path.relpath(temp_file)}'
                INTO TABLE orders_narrow
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY '\\n'
                (member_id, amount, order_date);
            """)
            conn.commit()
            print(f"✅ {idx:,} rows committed (last batch)")

    cur.close()
    conn.close()
    os.remove(temp_file)
    print("✅ 모든 데이터 적재 완료")


def load_member_to_db():
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="rootpass",
        database="perf_test",
        allow_local_infile=True
    )

    cur = conn.cursor()
    # members 테이블 적재 (한 번에)
    query_members = f"""
    LOAD DATA LOCAL INFILE '{os.path.relpath(MEMBERS_CSV)}'
    INTO TABLE members
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\\n'
    (member_id, name, email, created_at);
    """
    cur.execute(query_members)
    conn.commit()
    print("✅ members 적재 완료")
    return cur


if __name__ == "__main__":
    generate_orders_since_1_year_ago()
    load_orders_since_1_year_ago()
