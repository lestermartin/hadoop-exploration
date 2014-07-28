using Microsoft.Hadoop.MapReduce;
using System;
using System.Collections.Generic;

namespace HadoopExploration
{
    class WordCount
    {
        //see blog posting for this code at https://martin.atlassian.net/wiki/x/CAChAQ

        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                throw new ArgumentException("Usage: WordCount <input path> <output folder>");
            }

            HadoopJobConfiguration jobConfig = new HadoopJobConfiguration()
            {
                InputPath = args[0],
                OutputFolder = args[1],
                DeleteOutputFolder = true
            };

            Hadoop.Connect().MapReduceJob.Execute<WordMapper, SumReducer>(jobConfig);
        }
    }
}