# Commands

Run from the repository root.

```powershell
# Full isolated three-month rebuild
python .\isolated_long_3m_research_20260730\research.py research --rebuild

# Re-run compression refinement from the isolated cache
python .\isolated_long_3m_research_20260730\refinement.py

# Rebuild the main Markdown report from saved results
python .\isolated_long_3m_research_20260730\research.py report

# Verification
python -m unittest -v isolated_long_3m_research_20260730.test_research
python -m py_compile .\isolated_long_3m_research_20260730\research.py
python -m py_compile .\isolated_long_3m_research_20260730\refinement.py
```

Every generated artifact stays under
`isolated_long_3m_research_20260730/outputs`.
