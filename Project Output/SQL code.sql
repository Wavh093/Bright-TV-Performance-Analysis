select * from USERPROFILES;
--------------------------------------------------------------------------------------
-- droppping nulls from key columns (IF NEEDED, BUT NULLS WILL BE HANDLED MANUALLY)
CREATE TABLE user_profiles_cleaned AS
SELECT *
FROM userprofiles
WHERE userid IS NOT NULL
AND name IS NOT NULL
AND surname IS NOT NULL
AND gender IS NOT NULL
AND race IS NOT NULL
AND age IS NOT NULL
AND province IS NOT NULL
AND email_cleaned IS NOT NULL
AND social_media_handle_cleaned IS NOT NULL
AND age_group IS NOT NULL;

---------------------------------------------------------------------------------------
-- Conversion from UTC to SAST (+2 Hours)

SELECT *
FROM brighttv.data.viewership;

UPDATE brighttv.data.viewership
SET recorddate2 = DATEADD(HOUR, 2, recorddate2);

---------------------------------------------------------------------------------------
-- is weekend flag
SELECT user_id, channel2, DAYNAME(recorddate2) AS day_name,
    CASE
        WHEN day_name IN ('Sat','Sun') THEN 'Weekend'
        ELSE 'Weekday'
    END AS time_of_week
FROM brighttv.data.viewership;

-- adding it as a new column
ALTER TABLE brighttv.data.viewership
ADD COLUMN time_of_week STRING;

UPDATE brighttv.data.viewership
    SET time_of_week =
    CASE
        WHEN DAYNAME(recorddate2) IN ('Sat','Sun') THEN 'Weekend'
        ELSE 'Weekday'
    END;

--------------------------------------------------------------------------------------
-- Adding show_category
SELECT user_id, channel2,
    CASE 
        WHEN channel2 IN ('Trace TV', 'Channel O', 'Vuzu','MK','E! Entertainment') THEN 'Music & Entertainment'
        WHEN channel2 IN ('M-Net', 'Africa Magic','KykNET') THEN 'Movies'
        WHEN channel2 = 'CNN' THEN 'News & Current affairs'
        WHEN channel2 IN ('Supersport Live Events','Live on SuperSport','SuperSport Blitz','ICC Cricket World Cup 2011','Wimbledon') THEN 'Sports'
        WHEN channel2 IN ('Cartoon Network','Boomerang') THEN 'Kids & Family'
        WHEN channel2 IN ('SawSee','DStv Events 1') THEN 'Speciality & event channel'
        ELSE 'break in transmission'
    END AS channel_category
FROM brighttv.data.viewership;

-- adding it as a new column
ALTER TABLE brighttv.data.viewership
ADD COLUMN channel_category STRING;

UPDATE brighttv.data.viewership
    SET channel_category =
    CASE
        WHEN channel2 IN ('Trace TV', 'Channel O', 'Vuzu','MK','E! Entertainment') THEN 'Music & Entertainment'
        WHEN channel2 IN ('M-Net', 'Africa Magic','KykNET') THEN 'Movies'
        WHEN channel2 = 'CNN' THEN 'News & Current affairs'
        WHEN channel2 IN ('Supersport Live Events','Live on SuperSport','SuperSport Blitz','ICC Cricket World Cup 2011','Wimbledon') THEN 'Sports'
        WHEN channel2 IN ('Cartoon Network','Boomerang') THEN 'Kids & Family'
        WHEN channel2 IN ('SawSee','DStv Events 1') THEN 'Speciality & event channel'
        ELSE 'break in transmission'
    END;
        

---------------------------------------------------------------------------------------
-- EDA --
---------------------------------------------------------------------------------------

-- Count the number of users
SELECT DISTINCT COUNT(userid) AS tot_users
FROM brighttv.data.userprofiles;

-- get users per gender
SELECT DISTINCT gender, COUNT(userid) AS tot_male_users 
FROM brighttv.data.userprofiles
GROUP BY gender;

-- get users per race
SELECT DISTINCT race, COUNT(userid) AS tot_viewers_per_race  
FROM brighttv.data.userprofiles
GROUP BY race;

-- get users per age
SELECT age_group, COUNT(userid) AS age_groups  
FROM brighttv.data.userprofiles
GROUP BY age_group;

-- get users per province
SELECT province, COUNT(userid) AS tot_per_province  
FROM brighttv.data.userprofiles
GROUP BY province;

-- Count the number of channels
SELECT DISTINCT v.channel2,
COUNT(u.userid) AS total_viewers
FROM brighttv.data.viewership v
    INNER JOIN brighttv.data.userprofiles u
    ON v.user_id = u.userid
GROUP BY channel2
ORDER BY total_viewers DESC;

-- get max, min durations
SELECT MAX(duration_2) AS longest_duration
FROM brighttv.data.viewership;

SELECT MIN(duration_2) AS shortest_duration
FROM brighttv.data.viewership;


--------------------------------------------------------------------------
-- Deeper analysis (user profiles)
--------------------------------------------------------------------------
-- counting total users over time

--------------------------------------------------------------------------
-- returning vs one-time users

--------------------------------------------------------------------------
-- activity tiers (heavy, moderate, and light viewers)

--------------------------------------------------------------------------


--------------------------------------------------------------------------
-- Deeper analysis (content performance analysis)
--------------------------------------------------------------------------
-- Total watch per genre
SELECT 
  channel_category,
  SUM(DATE_PART('hour', duration_2) * 3600 +
      DATE_PART('minute', duration_2) * 60 +
      DATE_PART('second', duration_2)) / 3600 AS total_watchtime_hours
FROM brighttv.data.viewership
GROUP BY channel_category
ORDER BY total_watchtime_hours DESC;

--------------------------------------------------------------------------
-- average watch time per genre
SELECT 
  channel_category,
  AVG(DATE_PART('hour', duration_2) * 3600 +
      DATE_PART('minute', duration_2) * 60 +
      DATE_PART('second', duration_2)) / 60 AS avg_minutes
FROM brighttv.data.viewership
GROUP BY channel_category
ORDER BY avg_minutes DESC;
--------------------------------------------------------------------------
-- top 5 titles by total watch time
SELECT 
  channel2,
  SUM(DATE_PART('hour', duration_2) * 3600 +
      DATE_PART('minute', duration_2) * 60 +
      DATE_PART('second', duration_2)) / 3600 AS total_watchtime_hours
FROM brighttv.data.viewership
GROUP BY channel2
ORDER BY total_watchtime_hours DESC;

--------------------------------------------------------------------------
-- time based consumption patterns
--------------------------------------------------------------------------
-- content popularity by day of week (ROUNDED BY 2 DECIMAL PLACES)

SELECT 
  DAYNAME(recorddate2) AS day_name,
  ROUND(SUM(DATE_PART('hour', duration_2) * 3600 +
      DATE_PART('minute', duration_2) * 60 +
      DATE_PART('second', duration_2)) / 3600,2) AS total_watchtime_hours
FROM brighttv.data.viewership
GROUP BY day_name
ORDER BY total_watchtime_hours DESC;

--------------------------------------------------------------------------
-- content popularity by hour of day
SELECT 
  HOUR(recorddate2) AS hour_of_day,
  ROUND(SUM(DATE_PART('hour', duration_2) * 3600 +
      DATE_PART('minute', duration_2) * 60 +
      DATE_PART('second', duration_2)) / 3600,2) AS total_watchtime_hours
FROM brighttv.data.viewership
GROUP BY hour_of_day
ORDER BY total_watchtime_hours DESC;

--------------------------------------------------------------------------
-- watch time per month
SELECT 
  MONTHNAME(recorddate2) AS Month_name,
  ROUND(SUM(DATE_PART('hour', duration_2) * 3600 +
      DATE_PART('minute', duration_2) * 60 +
      DATE_PART('second', duration_2)) / 3600,2) AS total_watchtime_hours
FROM brighttv.data.viewership
GROUP BY Month_name
ORDER BY total_watchtime_hours DESC;

--------------------------------------------------------------------------
-- Weekend vs weekday metrics
SELECT 
  time_of_week,
  ROUND(SUM(DATE_PART('hour', duration_2) * 3600 +
      DATE_PART('minute', duration_2) * 60 +
      DATE_PART('second', duration_2)) / 3600,2) AS total_watchtime_hours
FROM brighttv.data.viewership
GROUP BY time_of_week
ORDER BY total_watchtime_hours DESC;

--------------------------------------------------------------------------
-- average number of sessions per user per day
SELECT DISTINCT DATE(recorddate2), COUNT(user_id) 
FROM brighttv.data.viewership
GROUP BY recorddate2;

--------------------------------------------------------------------------
-- Join operations
--------------------------------------------------------------------------
-- simple join
SELECT *
FROM brighttv.data.viewership v
INNER JOIN brighttv.data.user_profiles_cleaned u 
ON v.user_id = u.userid;

--------------------------------------------------------------------------
-- age group preferences
SELECT u.age_group, v.channel2, COUNT(*) AS view_count
FROM brighttv.data.viewership v
INNER JOIN brighttv.data.user_profiles_cleaned u 
ON v.user_id = u.userid
GROUP BY u.age_group, v.channel2
ORDER BY u.age_group, view_count DESC;

--------------------------------------------------------------------------
-- gender-based content preferences
SELECT u.gender, v.channel2, COUNT(*) AS view_count
FROM brighttv.data.viewership v
INNER JOIN brighttv.data.user_profiles_cleaned u 
ON v.user_id = u.userid
GROUP BY u.gender, v.channel2
ORDER BY u.gender, view_count DESC;

---------------------------------------------------------------------------
-- regional viewing trends
SELECT u.province, v.channel2, COUNT(*) AS view_count
FROM brighttv.data.viewership v
INNER JOIN brighttv.data.user_profiles_cleaned u 
ON v.user_id = u.userid
GROUP BY u.province, v.channel2
ORDER BY u.province, view_count DESC;

---------------------------------------------------------------------------
-- race based viewing trends
SELECT u.race, v.channel2, COUNT(*) AS view_count
FROM brighttv.data.viewership v
INNER JOIN brighttv.data.user_profiles_cleaned u 
ON v.user_id = u.userid
GROUP BY u.race, v.channel2
ORDER BY u.race, view_count DESC;

---------------------------------------------------------------------------
-- average viewership per age group
SELECT 
    u.age_group,
    ROUND(AVG(DATE_PART('hour', v.duration_2) * 3600 +
      DATE_PART('minute', v.duration_2) * 60 +
      DATE_PART('second', v.duration_2)) / 60,2) AS avg_minutes
FROM viewership v
INNER JOIN user_profiles_cleaned u
ON u.USERID = v.USER_ID
GROUP BY u.AGE_GROUP
ORDER BY avg_minutes DESC;

---------------------------------------------------------------------------
-- TOP channel by age group
SELECT AGE_GROUP, CHANNEL2
FROM (
  SELECT 
    u.AGE_GROUP,
    v.CHANNEL2,
    COUNT(*) AS view_count,
    ROW_NUMBER() OVER (PARTITION BY u.AGE_GROUP ORDER BY COUNT(*) DESC) AS rn
  FROM viewership v
  JOIN user_profiles_cleaned u ON v.USER_ID = u.USERID
  GROUP BY u.AGE_GROUP, v.CHANNEL2
) ranked
WHERE rn = 1;

----------------------------------------------------------------------------
-- top channel by race
SELECT race, CHANNEL2
FROM (
  SELECT 
    u.race,
    v.CHANNEL2,
    COUNT(*) AS view_count,
    ROW_NUMBER() OVER (PARTITION BY u.race ORDER BY COUNT(*) DESC) AS rn
  FROM viewership v
  JOIN user_profiles_cleaned u ON v.USER_ID = u.USERID
  GROUP BY u.race, v.CHANNEL2
) ranked
WHERE rn = 1;

----------------------------------------------------------------------------
-- top channel by province
SELECT province, CHANNEL2
FROM (
  SELECT 
    u.province,
    v.CHANNEL2,
    COUNT(*) AS view_count,
    ROW_NUMBER() OVER (PARTITION BY u.province ORDER BY COUNT(*) DESC) AS rn
  FROM viewership v
  JOIN user_profiles_cleaned u ON v.USER_ID = u.USERID
  GROUP BY u.race, v.CHANNEL2
) ranked
WHERE rn = 1;

-----------------------------------------------------------------------------
-- printing final tables for Excel
SELECT * FROM viewership;
SELECT * FROM user_profiles_cleaned;