
REGISTER file:/home/jack/Desktop/ca4022/pig-0.17.0/lib/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Load in our datasets

ratings = LOAD 'input/ratings.csv' USING CSVLoader(',') AS
(userID:int, movieID:int, rating:double, timestamp:int);
ratings = FILTER ratings BY userID is not null; -- remove first line (headers)

movies = LOAD 'input/movies.csv' USING PigStorage('\t') AS
(movieID:int, title:chararray, year:int, genre:chararray);
movies = FILTER movies BY movieID is not null;

tags = LOAD 'input/tags.csv' USING CSVLoader(',') AS
(userID:int, movieID:int, tag:chararray, timestamp:int);
tags = FILTER tags BY userID is not null;

links = LOAD 'input/links.csv' USING CSVLoader(',') AS
(movieID:int, imdbID:int, tmdbID:int);
links = FILTER links BY movieID is not null;

-- Make results folder
fs -rm -r -f output/movielens_assignment/results -- remove old dir
fs -mkdir -p output/movielens_assignment/results

-- few = limit movies 10;
-- dump few;

-- group each rating instance by movie IDs

ratingsByMovie = GROUP ratings BY movieID;


-- get average rating per movie ID

avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID,
AVG(ratings.rating) AS avgRating;


-- get number of ratings per movie ID

countRatings = FOREACH ratingsByMovie GENERATE group AS movieID,
COUNT(ratings.rating) AS numRating;


-- filter movies 4 stars or better

fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;


-- join best movies with name table to get title

fiveStarsWithData = JOIN fiveStarMovies BY movieID, movies BY movieID;

oldestFiveStars = ORDER fiveStarsWithData BY movies::year ASC;
oldestFiveStars = FILTER oldestFiveStars BY movies::year is not null;

DUMP oldestFiveStars;

store oldestFiveStars into 'output/movielens_assignment/results/oldestFiveStars' using PigStorage('\t', '-schema');
fs -rm -f output/movielens_assignment/results/oldestFiveStars/.pig_schema
fs -rm -f output/movielens_assignment/results/oldestFiveStars/_SUCCESS

-- Merge into single file and get local copy
fs -getmerge output/movielens_assignment/results/oldestFiveStars output/movielens_assignment/results/oldestFiveStars.csv;

-- Remove hdfs folder
fs -rm -r -f output/movielens_assignment/results/oldestFiveStars;


-- get the movie names of most rated movies

mostRatedTitles = JOIN countRatings BY movieID, movies BY movieID;

mostRatedTitles = ORDER mostRatedTitles BY countRatings::numRating DESC;

DUMP mostRatedTitles;
DESCRIBE mostRatedTitles;

store mostRatedTitles into 'output/movielens_assignment/results/mostRatedTitles' using PigStorage('\t', '-schema');
fs -rm -f output/movielens_assignment/results/mostRatedTitles/.pig_schema
fs -rm -f output/movielens_assignment/results/mostRatedTitles/_SUCCESS

-- Merge into single file and get local copy
fs -getmerge output/movielens_assignment/results/mostRatedTitles output/movielens_assignment/results/mostRatedTitles.csv;

-- Remove hdfs folder
fs -rm -r -f output/movielens_assignment/results/mostRatedTitles;


-- get number of 5 star ratings per movie

fiveStarRatings = FILTER ratings BY rating == 5;
fiveStarRatings = GROUP fiveStarRatings BY movieID;
FiveStarCount = FOREACH fiveStarRatings GENERATE group AS movieID,
COUNT(fiveStarRatings.rating) AS numFiveStarRating;



-- get movies where the majority of ratings are 5 stars
-- over 50% of ratings are 5 stars
-- this includes movies with 1 five star rating

FiveStarMajority = JOIN FiveStarCount BY movieID, countRatings BY movieID;
FiveStarMajority = filter FiveStarMajority BY
FiveStarCount::numFiveStarRating > countRatings::numRating / 2;

-- join movies table to get titles

FiveStarMajority = JOIN FiveStarMajority BY FiveStarCount::movieID, movies BY movieID;

-- exclude movies with less than 10 total ratings

FiveStarMajority = FILTER FiveStarMajority BY countRatings::numRating > 5;

DUMP FiveStarMajority;
DESCRIBE FiveStarMajority;

store FiveStarMajority into 'output/movielens_assignment/results/FiveStarMajority' using PigStorage('\t', '-schema');
fs -rm -f output/movielens_assignment/results/FiveStarMajority/.pig_schema
fs -rm -f output/movielens_assignment/results/FiveStarMajority/_SUCCESS

-- Merge into single file and get local copy
fs -getmerge output/movielens_assignment/results/FiveStarMajority output/movielens_assignment/results/FiveStarMajority.csv;

-- Remove hdfs folder
fs -rm -r -f output/movielens_assignment/results/FiveStarMajority;


-- find user with the highest average rating

ratingsByUser = GROUP ratings BY userID;

avgUserRatings = FOREACH ratingsByUser GENERATE group AS userID,
AVG(ratings.rating) AS avgRating,
COUNT(ratings.rating) AS numRating;

avgUserRatings = ORDER avgUserRatings BY avgRating DESC;

DUMP avgUserRatings;
DESCRIBE avgUserRatings;

store avgUserRatings into 'output/movielens_assignment/results/avgUserRatings' using PigStorage('\t', '-schema');
fs -rm -f output/movielens_assignment/results/avgUserRatings/.pig_schema
fs -rm -f output/movielens_assignment/results/avgUserRatings/_SUCCESS

-- Merge into single file and get local copy
fs -getmerge output/movielens_assignment/results/avgUserRatings output/movielens_assignment/results/avgUserRatings.csv;

-- Remove hdfs folder
fs -rm -r -f output/movielens_assignment/results/avgUserRatings;



-- find average ratings for movies of certain genre

actionWarMovies = FILTER movies BY (genre MATCHES '.*Action.*' OR genre MATCHES '.*War.*');

actionWarMovies = JOIN actionWarMovies BY movieID, avgRatings BY movieID, countRatings BY movieID;

actionWarMovies = FOREACH actionWarMovies GENERATE
actionWarMovies::movieID as movieID,
actionWarMovies::title as title,
avgRatings::avgRating as avgRating,
countRatings::numRating as numRating;

-- filter for movies with more than 5 ratings
-- and order highest to lowest

popularActionWarMovies = FILTER actionWarMovies BY numRating >= 5;
popularActionWarMovies = ORDER popularActionWarMovies BY avgRating DESC;

DUMP popularActionWarMovies;
DESCRIBE popularActionWarMovies;


store popularActionWarMovies into 'output/movielens_assignment/results/popularActionWarMovies' using PigStorage('\t', '-schema');
fs -rm -f output/movielens_assignment/results/popularActionWarMovies/.pig_schema
fs -rm -f output/movielens_assignment/results/popularActionWarMovies/_SUCCESS

-- Merge into single file and get local copy
fs -getmerge output/movielens_assignment/results/popularActionWarMovies output/movielens_assignment/results/popularActionWarMovies.csv;

-- Remove hdfs folder
fs -rm -r -f output/movielens_assignment/results/popularActionWarMovies;
