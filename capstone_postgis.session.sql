INSERT INTO analytics_contest (date, name, federation, country, city_name)
SELECT powerlifting_contest.date,
    powerlifting_contest.name,
    powerlifting_contest.federation,
    powerlifting_contestlocation.country,
    CASE
        WHEN lc.city IS NOT NULL THEN lc.city || ' - ' || lc.state
        ELSE NULL
    END AS city_name
FROM powerlifting_contest
    JOIN powerlifting_contestlocation ON powerlifting_contest.location_id = powerlifting_contestlocation.id
    LEFT JOIN (
        SELECT *,
            RANK() OVER (
                PARTITION BY powerlifting_contestlocation.country,
                powerlifting_contestlocation.state
                ORDER BY levenshtein(
                        powerlifting_contestlocation.town,
                        location_citylocation.city
                    )
            ) AS distance_rank
        FROM location_citylocation
            LEFT JOIN powerlifting_contestlocation ON powerlifting_contestlocation.country = location_citylocation.country
            AND LOWER(powerlifting_contestlocation.state) = LOWER(location_citylocation.city)
    ) lc ON powerlifting_contestlocation.country = lc.country
    AND LOWER(powerlifting_contestlocation.state) = LOWER(lc.city)
    AND lc.distance_rank = 1;