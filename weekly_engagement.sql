SELECT 
    day_of_week,
    COUNT(*) AS total_interactions,
    ROUND(AVG(session_correct::numeric / NULLIF(session_seen, 0)) * 100, 2) AS daily_accuracy
FROM duolingo
GROUP BY day_of_week
ORDER BY total_interactions DESC;