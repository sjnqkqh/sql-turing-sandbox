import mysql.connector
import sys


def run_with_profile(query: str):
    # DB 연결
    conn = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="testuser",
        password="testpass",
        database="perf_test"
    )
    cur = conn.cursor()

    try:
        # profiling 활성화
        cur.execute("SET profiling = 1;")

        # 쿼리 실행
        cur.execute(query)
        # 결과 fetch는 필요시만 (예: SELECT COUNT(*)면 fetchone만)
        _ = cur.fetchall()

        # 쿼리 id 확인
        cur.execute("SHOW PROFILES;")
        profiles = cur.fetchall()
        last_id = profiles[-1][0]  # 마지막 실행된 쿼리 ID

        # 단계별 프로파일 조회
        cur.execute(f"SHOW PROFILE FOR QUERY {last_id};")
        profile_data = cur.fetchall()

        print(f"\n[쿼리 실행 프로파일] {query}\n")
        total_time = 0.0
        for stage, time in profile_data:
            print(f"{stage:<30} {time:.6f} sec")
            total_time += float(time)
        print(f"\n총 소요 시간: {total_time:.6f} sec")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # 실행 예시
    run_with_profile("EXPLAIN ANALYZE SELECT SQL_NO_CACHE amount FROM orders WHERE amount BETWEEN 500 AND 1000 ORDER BY amount DESC;")
    run_with_profile("EXPLAIN ANALYZE SELECT SQL_NO_CACHE member_id FROM orders WHERE member_id BETWEEN 500 AND 1000 ORDER BY member_id DESC;")

