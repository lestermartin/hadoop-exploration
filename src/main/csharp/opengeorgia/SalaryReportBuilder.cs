using System;

namespace OpenGeorgia
{
    public class SalaryReportBuilder
    {
        private string workingText;

        public SalaryReport buildFromInputCsvFormat(string textAsCsv)
        {
            workingText = textAsCsv;
            SalaryReport salaryReport = new SalaryReport();

            salaryReport.Name = parseNextValueAsString();
            salaryReport.Title = parseNextValueAsString();

            salaryReport.Salary = parseNextValueAsFloat();

            salaryReport.Travel = parseNextValueAsFloat();

            salaryReport.OrgType = parseNextValueAsString();
            salaryReport.Org = parseNextValueAsString();

            salaryReport.Year = int.Parse(workingText.Substring(0));

            //sanity check!!
            if (salaryReport.Year > 2050 || salaryReport.Year < 2000)
            {
                throw new ArgumentException("unable to correctly parse string '" + textAsCsv + "'");
            }

            return salaryReport;
        }

        private String parseNextValueAsString()
        {
            string nextValue = null;
            //field, might, or might not, start with double-quotes
            if (workingText.StartsWith("\""))
            {
                int endOfValue = workingText.IndexOf("\"", 1);
                nextValue = workingText.Substring(1, endOfValue-1);
                workingText = workingText.Substring(endOfValue + 2);
            }
            else
            {
                int endOfValue = workingText.IndexOf(",", 0);
                nextValue = workingText.Substring(0, endOfValue);
                workingText = workingText.Substring(endOfValue + 1);
            }
            return nextValue;
        }

        private float parseNextValueAsFloat()
        {
            String floatAsString = parseNextValueAsString();
            floatAsString = floatAsString.Replace("$", "");
            floatAsString = floatAsString.Replace(",", "");
            floatAsString = floatAsString.Replace(" ", "");

            return float.Parse(floatAsString);
        }

    }
}
