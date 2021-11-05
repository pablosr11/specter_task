# specter_task

## dependencies
- [Poetry](https://python-poetry.org/) for installing dependencies
- ```poetry install```

## how to run
```python main```

## Notes
- Periodically triggering the script would be handled outside the script itself. We could trigger periodically with cronjobs, a lambda etc.
- Not deployed. Depends on the use case. I'd start by deploying on a lambda/cloud function every N time, storing data (for starters) in AWS RDS.
