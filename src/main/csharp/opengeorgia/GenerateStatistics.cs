using Microsoft.Hadoop.MapReduce;
using System;
using System.Collections.Generic;

namespace OpenGeorgia
{
    class GenerateStatistics
    {

        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                throw new ArgumentException("Usage: GenerateStatistics <input path> <output folder>");
            }

            HadoopJobConfiguration jobConfig = new HadoopJobConfiguration()
            {
                InputPath = args[0], OutputFolder = args[1], DeleteOutputFolder = true
            };

            Hadoop.Connect().MapReduceJob.Execute<TitleMapper, SalaryStatisticsReducer>(jobConfig);

            System.Console.Read();  //using to catch console
        }
    }
}
