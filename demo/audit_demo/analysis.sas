data high_value;
  set source;
  if value > 250;
  label = "High";
  keep name value label;
run;

proc sort data=high_value out=sorted_high;
  by value;
run;
