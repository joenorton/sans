#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
sans_dir="$script_dir/sans_script"
demo_sans="$sans_dir/demo.sans"
input="$sans_dir/in.csv"
out_dir="$script_dir/out"

cat > "$demo_sans" <<'EOF'
sans 1.0

format $grpfmt do
  "HIGH" -> "High risk"
  "LOW" -> "Low risk"
  other -> ""
end

data enriched do
  from in do
    rename(b -> base_b)
    where(a + b >= 10)
  end

  c = a * base_b
  risk = if(c > 50, "HIGH", "LOW")
  level = if(risk == "HIGH", 2, 1)
  risk_label = put(risk, $grpfmt.)
  keep(a, base_b, c, risk, level, risk_label)
end

sort enriched -> enriched_s by risk level c nodupkey true

summary enriched_s -> stats do
  class risk level
  var c
end

select stats -> final keep(risk, level, c_mean)
EOF

cat > "$input" <<'EOF'
a,b
1,3
2,7
3,12
4,14
5,9
EOF

python -m sans check "$demo_sans" --out "$out_dir" --tables in="$input"
python -m sans run "$demo_sans" --out "$out_dir" --tables in="$input"
python -m sans verify "$out_dir"
