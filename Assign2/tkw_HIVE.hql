--Author: Shiva Govindaraju

--Create a Table for getting input data from the inputfile (3 lines)
drop table if exists wcdoc;
create table wcdoc(text string) 
row format delimited fields terminated by '\n' stored as textfile;
load data local inpath '${inputfilename}' overwrite into table wcdoc;

--Create a table for word-count pairs (2 lines)
drop table if exists wcount;
create table wcount as
SELECT word, COUNT(*) as count FROM wcdoc
LATERAL VIEW explode(split(text, '\\s')) TABLE AS word
GROUP BY word ORDER BY word;

--Create a table that has the top 100 words (3 lines)
drop table if exists tophundred;
create table tophundred as 
SELECT * from wcount sort by count DESC LIMIT 100;
insert overwrite local directory 'hive_output_tkw' 
row format delimited fields terminated by '\t' 
stored as textfile select * from tophundred;

--Create a tbale that hjas the top 100 words > 6 characters (3 lines)
drop table if exists sevplus;
create table sevplus as 
SELECT * from wcount where length(word) >6 sort by count DESC LIMIT 100;
insert overwrite local directory 'hive_output_tkw_sevenplus' 
row format delimited fields terminated by '\t'
stored as textfile select * from sevplus;

--Clean up all tables. (4 lines)
drop table if exists tophundred;
drop table if exists sevplus;
drop table if exists wcount;
drop table if exists wcdoc;

