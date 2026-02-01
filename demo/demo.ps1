# Requires: PowerShell 5+ and Python on PATH

$demoDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$sansDir = Join-Path $demoDir "sans_script"
$demoSans = Join-Path $sansDir "demo.sans"
$inputCsv = Join-Path $sansDir "in.csv"
$outDir = Join-Path $demoDir "out"

$script = @'
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
'@
$script | Set-Content $demoSans -Encoding utf8

$csvLines = @(
  "a,b"
  "1,3"
  "2,7"
  "3,12"
  "4,14"
  "5,9"
)
$csvLines | Set-Content $inputCsv

python -m sans check "$demoSans" --out "$outDir" --tables in="$inputCsv"
python -m sans run "$demoSans" --out "$outDir" --tables in="$inputCsv"
python -m sans verify "$outDir"
