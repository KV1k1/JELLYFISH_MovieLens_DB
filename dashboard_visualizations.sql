-- Rozdelenie hodnotení podľa pohlavia a času dňa
-- Ukazuje rozdelenie hodnotení podľa pohlavia a podľa toho, či je doobeda alebo poobede.
SELECT 
    dt.ampm AS time_period,
    du.gender,
    COUNT(fr.fact_ratingID) AS total_ratings
FROM fact_ratings fr
JOIN dim_time dt ON fr.dim_timeID = dt.dim_timeID
JOIN dim_users du ON fr.dim_userID = du.dim_userID
GROUP BY dt.ampm, du.gender
ORDER BY time_period, du.gender;

-- Celkové hodnotenia používateľov vs priemerné hodnotenie
-- Celkový počet hodnotení, ktoré používateľ udelil, a jeho priemerné hodnotenie.
SELECT 
    du.age_group,
    du.occupation,
    COUNT(fr.fact_ratingID) AS total_ratings,
    AVG(fr.rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_users du ON fr.dim_userID = du.dim_userID
GROUP BY du.age_group, du.occupation
ORDER BY total_ratings DESC;

-- Frekvencia hodnotenia filmov podľa rokov
SELECT 
    dd.year AS year,
    COUNT(fr.fact_ratingID) AS total_ratings,
    AVG(fr.rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_dates dd ON fr.dim_dateID = dd.dim_dateID
GROUP BY dd.year
ORDER BY dd.year;

-- Zmeny priemerného hodnotenia v priebehu času
SELECT 
    dd.year AS year,
    AVG(fr.rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_dates dd ON fr.dim_dateID = dd.dim_dateID
GROUP BY dd.year
ORDER BY dd.year;

-- Rozdelenie hodnotení podľa povolania
SELECT 
    du.occupation AS occupation,
    fr.rating AS rating,
    COUNT(fr.fact_ratingID) AS rating_count,
    AVG(fr.rating) AS avg_rating
FROM fact_ratings fr
JOIN dim_users du ON fr.dim_userID = du.dim_userID
GROUP BY du.occupation, fr.rating
ORDER BY occupation, rating;