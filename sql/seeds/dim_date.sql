-- =============================================================================
-- Seed dim_date for the full generation window plus 12 months forward.
-- The range 2022-07-01 → 2024-07-31 covers:
--   • 6 months before start_date (brand pre-existence period)
--   • 15 months of generation window (2023-01-01 → 2024-03-31)
--   • 12 months forward buffer for future-dated payouts / analytics
-- =============================================================================

INSERT INTO dim_date (
    date_day,
    year,
    quarter,
    month,
    week,
    day_of_week,
    is_weekend,
    month_start,
    month_end
)
SELECT
    d::DATE                                         AS date_day,
    EXTRACT(YEAR    FROM d)::SMALLINT               AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT               AS quarter,
    EXTRACT(MONTH   FROM d)::SMALLINT               AS month,
    EXTRACT(WEEK    FROM d)::SMALLINT               AS week,
    EXTRACT(DOW     FROM d)::SMALLINT               AS day_of_week,
    EXTRACT(DOW     FROM d) IN (0, 6)               AS is_weekend,
    DATE_TRUNC('month', d)::DATE                    AS month_start,
    (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE AS month_end
FROM
    GENERATE_SERIES(
        '2022-07-01'::DATE,
        '2024-07-31'::DATE,
        '1 day'::INTERVAL
    ) AS gs(d)
ON CONFLICT (date_day) DO NOTHING;
