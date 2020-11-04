
register file:/home/jack/Desktop/ca4022/pig-0.17.0/lib/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Load in our datasets

ratings = load 'input/ratings.csv' using CSVLoader(',') as
(userID:int, movieID:int, rating:double, timestamp:int);
ratings = filter ratings by userID is not null; -- remove first line (headers)

movies = load 'input/movies.csv' using CSVLoader(',') as
(movieID:int, title:chararray, genre:chararray);
movies = filter movies by movieID is not null; -- remove first line (headers)

tags = load 'input/tags.csv' using CSVLoader(',') as
(userID:int, movieID:int, tag:chararray, timestamp:int);
tags = filter tags by userID is not null; -- remove first line (headers)

links = load 'input/links.csv' using CSVLoader(',') as
(movieID:int, imdbID:int, tmdbID:int);
links = filter links by movieID is not null;

-- few = limit movies 10;
-- dump few;

movies = foreach movies generate movieID,
REGEX_EXTRACT(title, '([\\S ]+) \\(\\d{4}\\)', 1) as title,
REGEX_EXTRACT(title, '\\((\\d{4})\\)', 1) as year,
STRSPLIT(genre, '\\|') as genre;

-- Save csv

fs -rm -r -f output/movies -- remove old dir
store movies into 'output/movies' using PigStorage('\t', '-schema');
fs -rm -f output/movies/.pig_schema
fs -rm -f output/movies/_SUCCESS

-- Merge into single file
fs -getmerge output/movies output/movies.csv;
fs -rm -r -f output/movies
fs -rm -f output/.movies.csv.crc;