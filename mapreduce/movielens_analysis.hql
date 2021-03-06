
DROP TABLE IF EXISTS movies PURGE;
CREATE TABLE IF NOT EXISTS movies
(movieID INT, title STRING, year INT, genre STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA INPATH 'input/movies.csv'
OVERWRITE INTO TABLE movies;

DROP TABLE IF EXISTS ratings PURGE;
CREATE TABLE IF NOT EXISTS ratings
(userID INT, movieID INT, rating DOUBLE, tim INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH 'input/ratings.csv'
OVERWRITE INTO TABLE ratings;

DROP TABLE IF EXISTS tags PURGE;
CREATE TABLE IF NOT EXISTS tags
(userID INT, movieID INT, tag STRING, tim INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH 'input/tags.csv'
OVERWRITE INTO TABLE tags;

DROP TABLE IF EXISTS links PURGE;
CREATE TABLE IF NOT EXISTS links
(movieID INT, imdbID INT, tmdb INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH 'input/links.csv'
OVERWRITE INTO TABLE links;

-- set this so headers appear above output
set hive.cli.print.header=true;


-- get title and genre from table

SELECT title, genre
FROM movies
LIMIT 10;


-- get films with most genres

INSERT OVERWRITE LOCAL DIRECTORY '/home/jack/Desktop/ca4022/output/hive_analysis/mostDiverseMovies' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
select title, genre
from movies
order by length(genre) desc
limit 500;


-- ratings go up in 0.5s from 0.5 to 5 stars
select distinct rating from ratings;





-- get most reviewed movieIDs and ratings info
DROP VIEW IF EXISTS topMovieIDs;
CREATE VIEW topMovieIDs AS
SELECT movieID, COUNT(rating) AS ratingCount,
AVG(rating) AS avgRating,
max(rating) as bestRating,
min(rating) as worstRating
FROM ratings
GROUP BY movieID;


DROP VIEW IF EXISTS topMovieStats;
CREATE VIEW topMovieStats AS
SELECT m.movieID, m.title, ratingCount, avgRating, bestRating, worstRating, m.genre
FROM topMovieIDs t JOIN movies m ON t.movieID = m.movieID;


-- top 10 movies based on number of ratings
SELECT *
FROM topMovieStats t JOIN movies m ON t.movieID = m.movieID
ORDER BY ratingCount DESC
LIMIT 10;

INSERT OVERWRITE LOCAL DIRECTORY '/home/jack/Desktop/ca4022/output/hive_analysis/mostRatedMovies' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT t.*, m.year
FROM topMovieStats t JOIN movies m on t.movieID = m.movieID
ORDER BY ratingCount DESC
limit 500;


-- find the highest rated films seen more than 100 times
select avgRating, title
from topMovieStats
WHERE ratingCount > 100
ORDER BY avgRating DESC
LIMIT 10;



-- create view to count number of X star ratings per movie
DROP VIEW IF EXISTS ratingCounts;
CREATE VIEW IF NOT EXISTS ratingCounts AS
SELECT movieID,
 COUNT(case when 4<rating and rating<=5 then 1 else null end) AS fiveStarRatingCount,
 COUNT(case when 3<rating and rating<=4 then 1 else null end) AS fourStarRatingCount,
 COUNT(case when 2<rating and rating<=3 then 1 else null end) AS threeStarRatingCount,
 COUNT(case when 1<rating and rating<=2 then 1 else null end) AS twoStarRatingCount,
 COUNT(case when 0<rating and rating<=1 then 1 else null end) AS oneStarRatingCount,
 COUNT(rating) AS ratingCount
FROM ratings
GROUP BY movieID;



-- the films with majority five star ratings and more than 3 ratings 
INSERT OVERWRITE LOCAL DIRECTORY '/home/jack/Desktop/ca4022/output/hive_analysis/majorityFiveStars' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
SELECT m.title, fiveStarRatingCount, fourStarRatingCount, threeStarRatingCount,
twoStarRatingCount, oneStarRatingCount, ratingCount
FROM ratingCounts f JOIN movies m ON f.movieID = m.movieID
WHERE fiveStarRatingCount > ratingCount / 2 AND ratingCount > 3
ORDER BY ratingCount DESC;


-- rating breakdown of all movies, starting with most rated 
INSERT OVERWRITE LOCAL DIRECTORY '/home/jack/Desktop/ca4022/output/hive_analysis/ratingBreakdown' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
SELECT m.title, fiveStarRatingCount, fourStarRatingCount, threeStarRatingCount,
twoStarRatingCount, oneStarRatingCount, ratingCount
FROM ratingCounts f JOIN movies m ON f.movieID = m.movieID
ORDER BY ratingCount DESC;



-- total number of each rating given
INSERT OVERWRITE LOCAL DIRECTORY '/home/jack/Desktop/ca4022/output/hive_analysis/totalRatings' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
SELECT sum(fiveStarRatingCount) as fiveStars, sum(fourStarRatingCount) as fourStars, 
sum(threeStarRatingCount) as threeStars, sum(twoStarRatingCount) as twoStars, sum(oneStarRatingCount) as oneStar
from ratingCounts;



-- find users with highest avg rating
INSERT OVERWRITE LOCAL DIRECTORY '/home/jack/Desktop/ca4022/output/hive_analysis/nicestUsers' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
SELECT userID,
 count(rating) as ratingCount,
 AVG(rating) as avgRating
FROM ratings
GROUP BY userID
ORDER BY avgRating DESC;




-- add python script so that its available to mapper and reducers
add file /home/jack/Desktop/ca4022/mapreduce/genre_col_to_row.py;

-- process our movie-rating data to get rows of genre and columns of info
INSERT OVERWRITE LOCAL DIRECTORY '/home/jack/Desktop/ca4022/output/hive_analysis/genreBreakdown' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
SELECT TRANSFORM(*) using "python3 genre_col_to_row.py" as (genre string, numMoviesInGenre int, avgRatings double, numRatings int)
FROM topMovieStats;


-- find most frequent tags
SELECT count(tag) as occurrence, tag
FROM tags
GROUP BY tag
ORDER BY occurrence DESC
LIMIT 10;


-- Which movies (titles) are the 10 most frequently tagged and how often have they been tagged?
select count(t.tag) as TagCount, m.title as title
from tags as t join movies as m on ( m.movieID = t.movieID)
GROUP BY m.movieID, m.title
ORDER BY TagCount DESC
LIMIT 10;



/* Which 10 movies were the most controversial in 2015 (i.e., had the highest variance in
ratings between 2015/01/01 and 2015/12/31)? 
*/
INSERT OVERWRITE LOCAL DIRECTORY '/home/jack/Desktop/ca4022/output/hive_analysis/controversialMovies' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT variance(r.rating) AS RatingVariance, m.title AS title,
m.movieID FROM ratings r , movies m
WHERE m.movieID = r.movieID
GROUP BY m.movieID, m.title
ORDER BY RatingVariance DESC
LIMIT 10;



-- process our rating data to get daily ratings info
INSERT OVERWRITE LOCAL DIRECTORY '/home/jack/Desktop/ca4022/output/hive_analysis/dailyRatings' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT date(from_unixtime(tim)) as newdate,
from_unixtime(tim, 'EEE') as dayOfWeek,
AVG(rating) AS avgRating,
COUNT(rating) AS ratingCount
FROM ratings
group by date(from_unixtime(tim)), from_unixtime(tim, 'EEE')
order by date(from_unixtime(tim));

drop view if exists dailyratings;
create view dailyratings as 
SELECT date(from_unixtime(tim)) as newdate,
from_unixtime(tim, 'EEE') as dayOfWeek,
AVG(rating) AS avgRating,
COUNT(rating) AS ratingCount
FROM ratings
group by date(from_unixtime(tim)), from_unixtime(tim, 'EEE')
order by date(from_unixtime(tim));



