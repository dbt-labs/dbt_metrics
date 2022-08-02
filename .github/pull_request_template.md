
## What is this PR?
This is a:
- [ ] documentation update
- [ ] bug fix with no breaking changes
- [ ] new functionality
- [ ] a breaking change

All pull requests from community contributors should target the `main` branch (default).

## Description & motivation
<!---
Describe your changes, and why you're making them. Be as descriptive as possible!
-->

## Checklist
- [ ] I have verified that these changes work locally on the following warehouses (Note: it's okay if you do not have access to all warehouses, this helps us understand what has been covered)
    - [ ] BigQuery
    - [ ] Postgres
    - [ ] Redshift
    - [ ] Snowflake
- [ ] I have updated the README.md (if applicable)
- [ ] I have added tests & descriptions to my models (and macros if applicable)
- [ ] I have added an entry to CHANGELOG.md

---
### Tenets to keep in mind 
- A metric value should be consistent everywhere that it is referenced
- We prefer generalized metrics with many dimensions over specific metrics with few dimensions
- It should be easier to use dbt’s metrics than it is to avoid them
- Organization and discoverability are as important as precision
- One-off models built to power metrics are an anti-pattern
