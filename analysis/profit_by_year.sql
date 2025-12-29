SELECT order_year, SUM(total_profit) AS profit_by_year
FROM pei.gold.agg_sales_performance
GROUP BY order_year