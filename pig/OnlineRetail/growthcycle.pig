yr2000 = LOAD 'Documents/pig/retail/2000.txt' USING PigStorage(',') AS (catid:int, catname:chararray, jan:int, feb:int, mar:int, apr:int, may:int, june:int, july:int, aug:int, sept:int, oct:int, nov:int, dec:int);

sum_months2000 = FOREACH yr2000 GENERATE $0, $1, jan + feb + mar + apr + may + june + july+ aug + sept + oct + nov + dec as total;

yr2001 = LOAD 'Documents/pig/retail/2001.txt' USING PigStorage(',') AS (catid:int, catname:chararray, jan:int, feb:int, mar:int, apr:int, may:int, june:int, july:int, aug:int, sept:int, oct:int, nov:int, dec:int);

sum_months2001 = FOREACH yr2001 GENERATE $0, $1, jan + feb + mar + apr + may + june + july+ aug + sept + oct + nov + dec as total;

yr2002 = LOAD 'Documents/pig/retail/2002.txt' USING PigStorage(',') AS (catid:int, catname:chararray, jan:int, feb:int, mar:int, apr:int, may:int, june:int, july:int, aug:int, sept:int, oct:int, nov:int, dec:int);

sum_months2002 = FOREACH yr2002 GENERATE $0, $1, jan + feb + mar + apr + may + june + july+ aug + sept + oct + nov + dec as total;

join_00_01_02 = JOIN sum_months2000 BY catid, sum_months2001 BY catid, sum_months2002 BY catid;

onlytotal = FOREACH join_00_01_02 GENERATE $0,$1,$2,$5,$8;

growthcycle = FOREACH onlytotal GENERATE $0, $1, (double)($3-$2)/$2*100, (double)($4-$3)/$3*100;

avg_growthcycle = FOREACH growthcycle GENERATE $0, $1, ($2+$3)/2;

cat_morethan10 = FILTER avg_growthcycle BY $2>=10.0;

cat_dropmorethan5 = FILTER avg_growthcycle BY $2<=-5.0;

entiretotal = FOREACH onlytotal GENERATE $0, $1, $2+$3+$4 as tot;

top_5 = LIMIT (ORDER entiretotal BY tot DESC) 5;

bottom_5 = LIMIT (ORDER entiretotal BY tot) 5;

----query1--
STORE cat_morethan10 INTO 'Documents/pig/retail/growth10' USING PigStorage(','); 
----query2--
STORE cat_dropmorethan5 INTO 'Documents/pig/retail/drop5' USING PigStorage(',');

----query3--
STORE top_5 INTO 'Documents/pig/retail/top5' USING PigStorage(',');

STORE bottom_5 INTO 'Documents/pig/retail/bottom5' USING PigStorage(',');




