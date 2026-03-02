SELECT 
    EXTRACT(HOUR FROM timestamp) AS hour_of_day,
    -- Calculate average memory strength as a percentage
    ROUND((AVG(p_recall) * 100)::numeric, 2) AS avg_memory_strength,
    -- Calculate average session accuracy
    ROUND((SUM(session_correct)::numeric / NULLIF(SUM(session_seen), 0) * 100)::numeric, 2) AS session_accuracy,
    COUNT(*) AS total_interactions
FROM duolingo
GROUP BY hour_of_day
ORDER BY hour_of_day;