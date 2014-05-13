a = load '/user/hue/dryrun1/10K-greenEggs.txt';
b = foreach a generate flatten(TOKENIZE((chararray)$0)) as word;
c = group b by word;
d = foreach c generate COUNT(b), group as word;
e = ORDER d by word;

dump e;
--store e into '/user/hue/dryrun/pigout1';