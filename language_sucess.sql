SELECT 
    learning_language,
    COUNT(DISTINCT user_id) AS unique_learners,
    COUNT(*) AS total_lessons_taken,
    -- We take the average, multiply by 100, then cast to numeric for the ROUND function
    ROUND((AVG(p_recall) * 100)::numeric, 2) AS avg_recall_rate
FROM duolingo
GROUP BY learning_language
ORDER BY total_lessons_taken DESC;