# Run the analysis
$env:PYTHONPATH="../../sans"
python -m sans run analysis.sas --out out --tables source=data.csv

# List created artifacts
echo "`nCreated Artifacts in demo/audit_demo/out/:"
ls out | select Name

# Show a snippet of the evidence
echo "`nSnippet of runtime.evidence.json (Row Counts):"
$evidence = Get-Content out/runtime.evidence.json | ConvertFrom-Json
$evidence.step_evidence | select step_index, op, row_counts
