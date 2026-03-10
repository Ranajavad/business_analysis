/*
This query measures user engagement and learning performance depending on the day of the week.
questions: Which days of the week do users practice the most, and how well do they perform?

This helps understand:

Engagement patterns

Weekend vs weekday learning

Best time for notifications or reminders
*/
SELECT 
    day_of_week,
    COUNT(*) AS total_interactions,
    ROUND(AVG(session_correct::numeric / NULLIF(session_seen, 0)) * 100, 2) AS daily_accuracy
FROM duolingo
GROUP BY day_of_week
ORDER BY total_interactions DESC;