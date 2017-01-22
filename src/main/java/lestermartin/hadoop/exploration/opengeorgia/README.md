Open Georgia: MapReduce
=======================

This code solves the [Simple Open Georgia Use Case](https://martin.atlassian.net/wiki/x/QYBmAQ) using the following classes.  See [Calculating Salary Statistics with MapReduce](https://martin.atlassian.net/wiki/x/SYBmAQ) blog posting for more details.

### The Mapper

The `TitleMapper` class first takes each row of CSV data (see [Format & Sample Data for Open Georgia](https://martin.atlassian.net/wiki/x/NYBmAQ) for more details) that it is passed during invocation of the `map()` method and constructs a `SalaryReport` object using the crude & primitive parsing logic of `SalaryReportBuilder`.

Then it simply bails out if it doesn't meet the basic [Simple Open Georgia Use Case](https://martin.atlassian.net/wiki/x/QYBmAQ) criteria.  If it does get past this initial filtering, then it emits a KVP of the job title and the salary value that goes along with it.

### The Reducer

`SalaryStatisticsReducer` simply calculates the total number of people for the given job title along with the minimum/maximum/average statistics.

### The Driver

`GenerateStatistics` pulls it all together so the MapReduce job can be run.