proc format;
  value $grpfmt 'HIGH'='High risk' 'LOW'='Low risk' other='';
run;

data enriched;
  set in(keep=a b rename=(b=base_b) where=(a + b >= 10));
  c = a * base_b;
  grp = 'LOW';
  if c > 50 then grp = 'HIGH';
  select (grp);
    when ('HIGH') level = 2;
    otherwise level = 1;
  end;
  keep a base_b c grp level;
run;

proc sort data=enriched out=enriched_s nodupkey;
  by grp level a;
run;

proc summary data=enriched_s nway;
  class grp level;
  var c;
  output out=stats mean= / autoname;
run;

data final;
  set stats;
  keep grp level c_mean;
run;
