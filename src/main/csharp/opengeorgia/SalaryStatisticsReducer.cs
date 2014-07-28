using Microsoft.Hadoop.MapReduce;
using System;
using System.Collections.Generic;

namespace OpenGeorgia
{
    public class SalaryStatisticsReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            int numberOfPeopleWithThisJob = 0;
            double totalSalaryAmount = 0.0d;
            double minSalary = double.MaxValue;
            double maxSalary = double.MinValue;

            foreach (string salaryAsString in values)
            {
                float salary = float.Parse(salaryAsString);
                numberOfPeopleWithThisJob++;
                totalSalaryAmount += salary;

                if (salary < minSalary) { minSalary = salary; }
                if (salary > maxSalary) { maxSalary = salary; }
            }

            string combinedText = "{" + numberOfPeopleWithThisJob + "," +
                minSalary + "," + maxSalary + "," +
                totalSalaryAmount / numberOfPeopleWithThisJob + "}";

            context.EmitKeyValue(key, combinedText);
        }
    }
}
