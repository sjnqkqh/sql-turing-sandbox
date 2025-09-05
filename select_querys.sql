-- 좁은 범위 풀 스캔
EXPLAIN ANALYZE
SELECT DISTINCT m.*
FROM orders_2024_2025 o FORCE INDEX (`PRIMARY`)
         JOIN members m ON m.member_id = o.member_id
WHERE o.order_date >= '2024-12-01'
  AND o.order_date < '2025-01-01'
ORDER BY m.last_login_at;

-- 좁은 범위 인덱스 레인지 스캔
EXPLAIN ANALYZE
SELECT DISTINCT m.*
FROM orders_2024_2025 o FORCE INDEX (orders_narrow_order_date_member_id_index)
         JOIN members m ON m.member_id = o.member_id
WHERE o.order_date >= '2024-12-01'
  AND o.order_date < '2025-01-01'
ORDER BY m.last_login_at;

-- 넓은 범위 풀 스캔
EXPLAIN ANALYZE
SELECT DISTINCT m.*
FROM orders_2019_2025 o FORCE INDEX (`PRIMARY`)
         JOIN members m ON m.member_id = o.member_id
WHERE o.order_date >= '2024-12-01'
  AND o.order_date < '2025-01-01'
ORDER BY m.last_login_at;

-- 넓은 범위 인덱스 레인지 스캔
EXPLAIN ANALYZE
SELECT DISTINCT m.*
FROM orders_2019_2025 o FORCE INDEX (orders_2019_2025_order_date_member_id_index)
         JOIN members m ON m.member_id = o.member_id
WHERE o.order_date >= '2024-12-01'
  AND o.order_date < '2025-01-01'
ORDER BY m.last_login_at;

-- IN 방식 SEMI JOIN
EXPLAIN ANALYZE
SELECT m.*
FROM members m
WHERE m.member_id IN
      (SELECT member_id
       FROM orders_2019_2025 o
       WHERE o.order_date >= '2024-12-01'
         AND o.order_date < '2025-01-01')
ORDER BY m.last_login_at;

-- EXISTS 방식 SEMI JOIN
EXPLAIN ANALYZE
SELECT m.*
FROM members m
WHERE EXISTS(SELECT 1
             FROM orders_2019_2025 o
             WHERE m.member_id = o.member_id
               AND o.order_date >= '2024-12-01'
               AND o.order_date < '2025-01-01')
ORDER BY m.last_login_at;

-- 비효율적 인덱스 설계
EXPLAIN ANALYZE
SELECT m.*
FROM orders_2024_2025 o FORCE INDEX (orders_2024_2025_order_date_index)
         JOIN members m ON m.member_id = o.member_id
WHERE o.order_date BETWEEN '2024-12-01' AND '2025-01-01'
ORDER BY m.last_login_at;

-- 좁은 범위 페이징
EXPLAIN ANALYZE
SELECT m.*
FROM members m
WHERE m.member_id IN
      (SELECT member_id
       FROM orders_2024_2025 o FORCE INDEX (orders_2024_2025_members_member_id_fk)
       WHERE o.order_date BETWEEN '2024-12-01' AND '2025-01-01'
	       AND o.member_id = m.member_id)
ORDER BY m.last_login_at
LIMIT 0, 50;

-- 넓은 범위 페이징
EXPLAIN ANALYZE
SELECT m.*
FROM members m
WHERE m.member_id IN
      (SELECT member_id
       FROM orders_2019_2025 o FORCE INDEX (orders_members_member_id_fk)
       WHERE o.order_date BETWEEN '2024-12-01' AND '2025-01-01'
	       AND o.member_id = m.member_id)
ORDER BY m.last_login_at
LIMIT 0, 50;
