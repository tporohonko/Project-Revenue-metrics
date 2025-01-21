WITH monthly_revenue AS (
    SELECT
        user_id,
        game_name,
        DATE_TRUNC('month', payment_date) AS payment_month,
        SUM(revenue_amount_usd) AS total_revenue
    FROM project.games_payments
    GROUP BY user_id, game_name, DATE_TRUNC('month', payment_date)
),
user_activity AS (
    SELECT
        gp.user_id,
        DATE_TRUNC('month', gp.payment_date) AS payment_month,
        COALESCE(LAG(SUM(gp.revenue_amount_usd)) OVER (PARTITION BY gp.user_id ORDER BY DATE_TRUNC('month', gp.payment_date)), 0) AS prev_month_revenue,
        SUM(gp.revenue_amount_usd) AS current_month_revenue,
        MAX(DATE_TRUNC('month', gp.payment_date)) OVER (PARTITION BY gp.user_id) AS last_paid_month
    FROM project.games_payments gp
    GROUP BY gp.user_id, DATE_TRUNC('month', gp.payment_date)
),
metrics AS (
    SELECT
        ma.user_id,
        ma.payment_month,
        SUM(ma.current_month_revenue) AS mrr,
        CASE
            WHEN ma.prev_month_revenue = 0 AND ma.current_month_revenue > 0 THEN 1
            ELSE 0
        END AS new_paid_users,
        CASE
            WHEN ma.prev_month_revenue = 0 AND ma.current_month_revenue > 0 THEN ma.current_month_revenue
            ELSE 0
        END AS new_mrr,
        CASE
            WHEN ma.last_paid_month = payment_month THEN 1
            ELSE 0
        END AS churned_users,
        CASE
            WHEN ma.last_paid_month = payment_month THEN ma.prev_month_revenue
            ELSE 0
        END AS churned_revenue,
        CASE
            WHEN ma.current_month_revenue > ma.prev_month_revenue THEN ma.current_month_revenue - ma.prev_month_revenue
            ELSE 0
        END AS expansion_mrr,
        CASE
            WHEN ma.current_month_revenue < ma.prev_month_revenue THEN ma.prev_month_revenue - ma.current_month_revenue
            ELSE 0
        END AS contraction_mrr
    FROM user_activity ma
    GROUP BY ma.user_id, ma.payment_month, ma.prev_month_revenue, ma.current_month_revenue, ma.last_paid_month
)
SELECT 
    m.user_id,
    TO_CHAR(m.payment_month, 'YYYY-MM') AS payment_month,
    m.mrr,
    m.new_paid_users,
    m.new_mrr,
    m.churned_users,
    m.churned_revenue,
    m.expansion_mrr,
    m.contraction_mrr,
    gpu."language",
    gpu.has_older_device_model,
    gpu.age 
FROM metrics m
LEFT JOIN project.games_paid_users gpu on m.user_id = gpu.user_id 
ORDER BY m.user_id, m.payment_month;
