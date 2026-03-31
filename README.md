
 Business Intelligence Analysis of Duolingo User Behavior

This project is a Business Intelligence (BI) and data analytics study based on approximately 13 million user interactions from a Duolingo spaced-repetition dataset. The main goal is to understand how users engage with the platform, how learning performance behaves over time, and which factors create difficulty in the learning process.

The analysis transforms raw interaction data into structured insights using a full data pipeline approach, including data cleaning, transformation, storage, and analysis in Python.



What I Did

I built an end-to-end data pipeline to process and analyze large-scale learning data.

The dataset was cleaned and prepared by converting timestamps into usable date-time features, extracting additional variables such as day of the week, and standardizing vocabulary entries. Missing values were handled to ensure consistent analysis, and the dataset was processed efficiently in chunks due to its size.

After cleaning, the data was structured into a database and used for exploratory analysis focusing on four key areas:

* User activity by hour of the day
* User engagement across days of the week
* Vocabulary difficulty at the word (lexeme) level
* Performance differences across languages


 Key Outcomes

The analysis revealed several important patterns:

User engagement is strongly time-dependent, with activity peaking in the evening (especially between 19:00 and 22:00). Mondays show the highest engagement during the week, while midweek activity is lower. Despite these fluctuations in activity, learning performance remains very stable across all hours and days.

Content difficulty is a more significant factor than timing. Certain vocabulary items consistently show lower accuracy, particularly words with multiple meanings such as “since” and irregular verbs like “went,” “were,” and “said.” These represent clear friction points in the learning process.

At the language level, English and Spanish dominate in usage, while Italian and Portuguese show slightly higher recall performance. French shows the lowest recall rate among the analyzed languages.

Overall, the results show that **what users learn has a bigger impact on performance than when they learn it**.



Learning outcomes
This project helped me understand how to work with large-scale datasets and how to structure a full BI data pipeline from raw data to insights.

I gained experience in:

* Handling large datasets efficiently using chunk processing
* Building ETL workflows (extract, transform, load)
* Working with time-based and behavioral data
* Identifying patterns in user engagement and learning performance
* Translating raw data into business insights that can support decision-making

Most importantly, I learned how data analysis can be used to identify real user friction points and how these insights can directly inform product and learning design improvements.

