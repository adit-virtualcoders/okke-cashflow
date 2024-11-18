# Okke Semantic Models dbt

dbt Project using SQL Server and Okke Data Warehouse.

## Configuration and Initialization

- Install `python==3.11` (`3.11.9` in this project)
- Install `pipenv` - https://python-poetry.org/docs/
- Ensure the system running dbt has connection to the data warehouse.
- Update credentials to the data warehouse in the `profiles.yml`.
- Install dbt dependencies via:

```
pipenv install
```

## Usage

```
dbt run
```
