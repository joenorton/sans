# Requires: PowerShell 5+ and Python on PATH

$scriptLines = @(
  "data out;",
  "  set in;",
  "  c = a + b;",
  "  if c > 20;",
  "run;"
)
$scriptLines | Set-Content hello.sas

$csvLines = @(
  "a,b",
  "1,10",
  "2,20",
  "3,30"
)
$csvLines | Set-Content in.csv

python -m sans check hello.sas --out out --tables in=in.csv
python -m sans run hello.sas --out out --tables in=in.csv
