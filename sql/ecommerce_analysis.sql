WITH account_metrics AS (
    SELECT
        DATE(s.date) as date,
        sp.country,
        a.send_interval,
        a.is_verified,
        a.is_unsubscribed,
        COUNT(a.id) AS account_cnt,
        0 AS sent_msg,
        0 AS open_msg,
        0 AS visit_msg
    FROM `DA.account` a
    JOIN `DA.account_session` acs ON a.id = acs.account_id
    JOIN `DA.session_params` sp ON acs.ga_session_id = sp.ga_session_id
    JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
    GROUP BY date, sp.country, a.send_interval, a.is_verified, a.is_unsubscribed
),

-- CTE для розрахунку метрик емейлів
email_metrics AS (
    SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) as sent_day,
        sp.country,
        0 as send_interval,
        0 as is_verified,
        0 as is_unsubscribed,
        0 AS account_cnt,
        COUNT(DISTINCT es.id_message) AS sent_msg,
        COUNT(DISTINCT eo.id_message) AS open_msg,
        COUNT(DISTINCT ev.id_message) AS visit_msg
    FROM `DA.email_sent` es
    LEFT JOIN `DA.email_open` eo ON es.id_message = eo.id_message
    LEFT JOIN `DA.email_visit` ev ON es.id_message = ev.id_message
    JOIN `DA.account_session` acs ON es.id_account = acs.account_id
    JOIN `DA.session` s ON acs.ga_session_id = s.ga_session_id
    JOIN `DA.session_params` sp ON acs.ga_session_id = sp.ga_session_id
    GROUP BY sent_day, sp.country
),

-- Об'єднання даних з обох CTE через UNION ALL
unified_metrics AS (
    SELECT * FROM account_metrics
    UNION ALL
    SELECT * FROM email_metrics
),

-- Агрегація даних після об'єднання
aggregated_metrics AS (
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        SUM(account_cnt) AS account_cnt,
        SUM(sent_msg) AS sent_msg,
        SUM(open_msg) AS open_msg,
        SUM(visit_msg) AS visit_msg
    FROM unified_metrics
    GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),

-- Загальні метрики по країнах з віконними функціями
total_country_metrics AS (
    SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        SUM(account_cnt) AS account_cnt,
        SUM(sent_msg) AS sent_msg,
        SUM(open_msg) AS open_msg,
        SUM(visit_msg) AS visit_msg,
        SUM(SUM(account_cnt)) OVER (PARTITION BY country) AS total_country_account_cnt,
        SUM(SUM(sent_msg)) OVER (PARTITION BY country) AS total_country_sent_cnt
    FROM unified_metrics
    GROUP BY date, country, send_interval, is_verified, is_unsubscribed
),


ranking AS (
    SELECT *,
        DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
        DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt
    FROM total_country_metrics
)


SELECT *
FROM ranking
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10
ORDER BY date, country;
