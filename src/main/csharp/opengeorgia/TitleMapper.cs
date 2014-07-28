using Microsoft.Hadoop.MapReduce;
using System;

namespace OpenGeorgia
{
    public class TitleMapper : MapperBase
    {

        private SalaryReportBuilder builder;

        public override void Initialize(MapperContext context)
        {
            builder = new SalaryReportBuilder();
            base.Initialize(context);
        }

        public override void Map(string inputLine, MapperContext context)
        {
            SalaryReport salaryReport = builder.buildFromInputCsvFormat(inputLine);

            if (!salaryReport.OrgType.Equals("LBOE")) { return; }  //ignore; not a school board
            if (salaryReport.Year != 2010) { return; }             //ignore, not right year

            context.EmitKeyValue(salaryReport.Title, Convert.ToString(salaryReport.Salary));
        }

    }
}