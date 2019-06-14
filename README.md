# Instructions

Here's the output of `$ python fk_detect.py -h`

```
$ python fk_detect.py -h
usage: fk_detect.py [-h] -t DBTYPE -H HOST -p PORT -u USER [--no-password]
                    [--password-file PASSWORD_FILE] [--ssl] [-d DB]
                    [-o OUTPUT] [-U]

Finds foreign keys in a database. This is accomplished with a combination of
using foreign key constraints and using column and table names. For example,
if a column is named user_id, and there is a table called user with an id
column, a foreign key will be reported.

optional arguments:
  -h, --help            show this help message and exit
  -t DBTYPE, --dbtype DBTYPE
                        type of DB to connect to, valid values are "mysql" and
                        "postgres"
  -H HOST, --host HOST  host to connect to
  -p PORT, --port PORT  port to connect to
  -u USER, --user USER  user to connect with
  --no-password         connect without a password. otherwise provide
                        onethrough a file with --password-file
  --password-file PASSWORD_FILE
                        location of a file that contains the password to the
                        database, see README.md for format
  --ssl                 force the connection over SSL
  -d DB, --db DB        database to connect to (optional in MySQL)
  -o OUTPUT, --output OUTPUT
                        output file, stdout is default
  -U, --union-constraints
                        output proposed foreign keys unioned with known
                        foreign key constraints. By default fk_detect does not
                        output foreign keys represented as constraints in the
                        database. Enabling this switch will union them to the
                        proposed constraints.
```

# Example

Example, using Postgres:

```
$ python fk_detect -t postgres -H localhost -p 5432 -u user -d test_data --password-file password-file
```

# Password file format

Use a single line with nothing but the password in it, e.g., if your password is `hunter2`, the following file should be used:

```
hunter2
```

