-- 데이터 주입 이전, INSERT 성능 최적화
SET FOREIGN_KEY_CHECKS = 0;
SET GLOBAL local_infile = 1;
SET GLOBAL innodb_flush_log_at_trx_commit = 2;
SET SESSION sql_log_bin = 0;

-- 예: 8 GiB로 늘리기
SET GLOBAL innodb_redo_log_capacity = 4 * 1024 * 1024 * 1024;
SET GLOBAL innodb_log_buffer_size = 64 * 1024 * 1024;

DROP DATABASE IF EXISTS perf_test;
CREATE DATABASE perf_test;
USE perf_test;

-- 회원 테이블
DROP DATABASE IF EXISTS perf_test;
CREATE DATABASE perf_test;
USE perf_test;

-- 회원 테이블
CREATE TABLE members
(
    member_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    email         VARCHAR(255) NOT NULL,
    last_login_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE = InnoDB;

-- 주문 테이블
CREATE TABLE orders
(
    order_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    member_id  BIGINT         NOT NULL,
    amount     DECIMAL(10, 2) NOT NULL,
    order_date DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE = InnoDB;

-- 좁은 주문일자 범위 테이블
CREATE TABLE orders_narrow
(
    order_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    member_id  BIGINT         NOT NULL,
    amount     DECIMAL(10, 2) NOT NULL,
    order_date DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE = InnoDB;


-- 회원 테이블 생성일시와 마지막 로그인 시점 필드 갱신
UPDATE members
SET created_at = TIMESTAMP(
        '2024-01-01 00:00:00'
            + INTERVAL FLOOR(RAND() * TIMESTAMPDIFF(SECOND, '2024-01-01 00:00:00', NOW())) SECOND);

UPDATE members
SET last_login_at = DATE_ADD(
        created_at,
        INTERVAL FLOOR(RAND() * TIMESTAMPDIFF(SECOND, created_at, NOW())) SECOND);

-- 인덱스 없는 members_copy 테이블 생성
# CREATE TABLE members_copy AS
# SELECT *
# FROM members;

-- orders 테이블 FK 및 인덱스 복구
ALTER TABLE orders_2019_2025 ADD CONSTRAINT fk_orders_member FOREIGN KEY (member_id) REFERENCES members (member_id);
CREATE INDEX idx_member_id ON orders_2019_2025 (member_id);
CREATE INDEX orders_order_date_member_id_index ON orders_2019_2025 (order_date DESC, member_id DESC);
CREATE INDEX orders_narrow_order_date_member_id_index ON orders_2024_2025 (order_date DESC, member_id DESC);

-- FK 체크 다시 활성화
SET FOREIGN_KEY_CHECKS = 1;
SET GLOBAL innodb_flush_log_at_trx_commit = 1;
SET GLOBAL local_infile = 0;