-- check out blog posting for this code at https://martin.atlassian.net/wiki/x/dIBmAQ

SELECT title, count(title) as count,
       MIN(salary) as min, MAX(salary) as max, AVG(salary) as avg
  FROM opengeorgia.salary
 where orgType = 'LBOE'
   and year = 2010
 group by title;