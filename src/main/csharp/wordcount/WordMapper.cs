using Microsoft.Hadoop.MapReduce;
using System;
using System.Collections.Generic;

namespace HadoopExploration
{
    public class WordMapper : MapperBase
    {
        //see blog posting for this code at https://martin.atlassian.net/wiki/x/CAChAQ

        public override void Map(string inputLine, MapperContext context)
        {
            char[] delimiterChars = { ' ', ',', '.', ':', '\t' };

            //split up the passed in line
            string[] individualWords = inputLine.Trim().Split(delimiterChars);

            //write output KVP for each one of these
            foreach (string word in individualWords)
            {
                context.EmitKeyValue(word.ToLower(), "1");
            }
        }
    }
}