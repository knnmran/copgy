# copgy

copgy is a CLI utility tool for copying data between and executing SQL on
PostgreSQL databases.

## Warning

Employ the **readonly user** approach when engaging with **production** databases to prevent inadvertent **data loss**.

## Features

- copy data using a single query
- copy data using a script json file
- copy data & execute SQL using a script json file

## Installation

```bash
cargo install copgy
```

## Usage

### Single

```bash
copgy --source-db-url postgresql://host:5432/postgres --dest-db-url postgresql://host:5432/postgres single --source-sql 'select * from employees' --dest-table employees_tmp
```

### Script

```bash
copgy --source-db-url postgresql://host:5432/postgres --dest-db-url postgresql://host:5432/postgres script --file-path ~/Desktop/copgy.json
```

sample copgy.json

```json
[
  {
    // execute on source db
    "execute": {
      "source_sql": "update employees set first_name = 'copgy' where emp_no = 0"
    }
  },
  {
    // execute on destination db
    "execute": {
      "dest_sql": "truncate employees_tmp"
    }
  },
  {
    // copy from source db to destination db
    "copy": {
      "source_sql": "select * from employees",
      "dest_table": "employees_tmp"
    }
  }
]
```

## Info

Sample PostgreSQL connection string

```bash
postgresql://username:password@host:port/dbname
```

## License

[MIT](https://choosealicense.com/licenses/mit/)
