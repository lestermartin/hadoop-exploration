using Microsoft.Hadoop.MapReduce;
using System;
using System.Collections.Generic;

namespace HadoopExploration
{
    public class SumReducer : ReducerCombinerBase
    {
        //see blog posting for this code at https://martin.atlassian.net/wiki/x/CAChAQ

        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            int wordCount = 0;

            //iterate through the count values passed to us by the mapper
            foreach (string countAsString in values)
            {
                wordCount += Int32.Parse(countAsString);
            }

            //write output "answer" as a KVP
            context.EmitKeyValue(key, Convert.ToString(wordCount));
        }
    }
}