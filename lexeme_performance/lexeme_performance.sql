/*
This query identifies the most difficult words for learners.
question: Which vocabulary words cause the most difficulty for learners?    
This helps:

Improve lesson design

Add extra practice for difficult words

Identify confusing vocabulary


*/
SELECT 
    lexeme_string AS word,
    SUM(session_seen) AS total_attempts,
    SUM(session_correct) AS total_correct,
    -- Calculate percentage of correct answers
    ROUND((SUM(session_correct)::numeric / NULLIF(SUM(session_seen), 0)) * 100, 2) AS accuracy_percentage
FROM duolingo
GROUP BY lexeme_string
HAVING SUM(session_seen) > 1000  -- Filters out rare words to get better averages
ORDER BY accuracy_percentage ASC
LIMIT 10;