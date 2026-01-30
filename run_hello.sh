#!/usr/bin/env bash
set -euo pipefail

cat > hello.sas <<'EOF'
data out;
  set in;
  c = a + b;
  if c > 20;
run;
EOF

cat > in.csv <<'EOF'
a,b
1,10
2,20
3,30
EOF

python -m sans check hello.sas --out out --tables in=in.csv
python -m sans run hello.sas --out out --tables in=in.csv
