Sales Leaders Crtieria
WHERE
    g.avg_growth_selected > 25       -- Strong projected revenue growth over next 2 years
    AND g.growth_t_plus_2 > 15       -- Atleast 15% growth in year after next
    AND sm.market_cap > 1,000,000,000   -- Focus on 1B+
    AND sm.fps < 100                  -- Exclude extreme outliers in forward price/share
    AND sm.industry <> 'Biotechnology' -- Exclude biotech sector



-- Define which years to average
WITH params AS (
    SELECT
        EXTRACT(YEAR FROM CURRENT_DATE)::int AS current_year,
        ARRAY[
            EXTRACT(YEAR FROM CURRENT_DATE)::int + 1,
            EXTRACT(YEAR FROM CURRENT_DATE)::int + 2
        ] AS avg_growth_years
),

revenue_growth AS (
    SELECT
        e.ticker,
        e.date,
        EXTRACT(YEAR FROM e.date)::int AS year,
        ROUND(
            (
                e.revenue_avg
                - LAG(e.revenue_avg) OVER (PARTITION BY e.ticker ORDER BY e.date)
            )::numeric
            / NULLIF(
                LAG(e.revenue_avg) OVER (PARTITION BY e.ticker ORDER BY e.date),
                0
            ) * 100,
            2
        ) AS yoy_revenue_growth_pct
    FROM analyst_estimates e
),

dynamic_growth AS (
    SELECT
        rg.ticker,
        MAX(CASE WHEN rg.year = p.current_year - 1 THEN rg.yoy_revenue_growth_pct END) AS growth_t_minus_1,
        MAX(CASE WHEN rg.year = p.current_year     THEN rg.yoy_revenue_growth_pct END) AS growth_t,
        MAX(CASE WHEN rg.year = p.current_year + 1 THEN rg.yoy_revenue_growth_pct END) AS growth_t_plus_1,
        MAX(CASE WHEN rg.year = p.current_year + 2 THEN rg.yoy_revenue_growth_pct END) AS growth_t_plus_2,
        ROUND(
            AVG(rg.yoy_revenue_growth_pct) FILTER (
                WHERE rg.year = ANY (p.avg_growth_years)
            ),
            2
        ) AS avg_growth_selected
    FROM revenue_growth rg
    CROSS JOIN params p
    GROUP BY rg.ticker
)

SELECT
    g.ticker,
    sm.company_name, 
    sm.industry, 
    sm.sector,
    g.growth_t_minus_1,
    g.growth_t,
    g.growth_t_plus_1,
    g.growth_t_plus_2,
    g.avg_growth_selected,
    sm.market_cap,
    sm.fpe,
    sm.dr_120,
    sm.fps
FROM dynamic_growth g
JOIN stock_metrics sm
    ON sm.ticker = g.ticker
WHERE
    g.avg_growth_selected > 25
    AND g.growth_t_plus_2 > 15
    AND sm.market_cap > 1000000000
    AND sm.fps < 100
    AND sm.industry <> 'Biotechnology'
ORDER BY sm.market_cap DESC;


