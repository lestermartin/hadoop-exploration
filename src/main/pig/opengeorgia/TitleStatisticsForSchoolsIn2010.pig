-- load up the base UDF (piggybank) and get a handle on the REPLACE function
register /user/hue/shared/pig/udfs/piggybank.jar;
define REPLACE org.apache.pig.piggybank.evaluation.string.REPLACE();

-- load the salary file and declare its structure
inputFile = LOAD '/user/hue/opengeorgia/salaryTravelReport.csv'
 using org.apache.pig.piggybank.storage.CSVExcelStorage()
 as (name:chararray, title:chararray, salary:chararray, travel:chararray, orgType:chararray, org:chararray, year:int);

-- loop thru the input data to clean up the number fields a bit
cleanedUpNumbers = foreach inputFile GENERATE
 name as name, title as title,
 (float)REPLACE(salary, ',','') as salary,  -- take out the commas and cast to a float
 (float)REPLACE(travel, ',','') as travel,  -- take out the commas and cast to a float
 orgType as orgType, org as org, year as year;

-- trim down to just Local Boards of Education
onlySchoolBoards = filter cleanedUpNumbers by orgType == 'LBOE';

-- further trim it down to just be for the year in question
onlySchoolBoardsFor2010 = filter onlySchoolBoards by year == 2010;

-- bucket them up by the job title
byTitle = GROUP onlySchoolBoardsFor2010 BY title;

-- loop through the titles and for each one...
salaryBreakdown = FOREACH byTitle GENERATE
 group as title, -- we grouped on this above
 COUNT(onlySchoolBoardsFor2010), -- how many people with this title
 MIN(onlySchoolBoardsFor2010.salary), -- determine the min
 MAX(onlySchoolBoardsFor2010.salary), -- determine the max
 AVG(onlySchoolBoardsFor2010.salary); -- determine the avg

-- guarantee the order on the way out
sortedSalaryBreakdown = ORDER salaryBreakdown by title;

-- dump results to the UI
--dump sortedSalaryBreakdown;

-- save results back to HDFS
STORE sortedSalaryBreakdown into '/user/hue/opengeorgia/pigoutput';