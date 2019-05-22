import argparse, sys
import db_interface
from fuzzywuzzy import fuzz
from pprint import pprint

def parse_args():
    description = 'Finds foreign keys in a database. This is accomplished with a combination of' \
        + ' using foreign key constraints and using column and table names. For example, if a column is named' \
        + ' user_id, and there is a table called user with an id column, a foreign key will be reported.'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-t', '--dbtype', help='type of DB to connect to, valid values are "mysql" and "postgres"', required=True)
    parser.add_argument('-H', '--host', help='host to connect to', required=True)
    parser.add_argument('-p', '--port', help='port to connect to', required=True)
    parser.add_argument('-u', '--user', help='user to connect with', required=True)
    parser.add_argument('--no-password', help='connect without a password. otherwise provide one with FK_DETECT_PASSWORD environment variable',
        action='store_true')
    parser.add_argument('--ssl', help='force the connection over SSL', action='store_true')
    parser.add_argument('-d', '--db', help='database to connect to (optional in MySQL)')
    parser.add_argument('-o', '--output', help='output file, stdout is default', required=False)
    return parser.parse_args()

def fk_name_heuristic(columns, primary_keys):
    retval = []
    n_columns = len(columns)
    count = 0
    count_down = 9
    print('Searching for fuzzy column name matches. Finished in 10.. ', end='')
    sys.stdout.flush()
    for col in columns:
        col_name = col[2]
        count += 1

        if count / n_columns > 0.1:
            print(str(count_down) + '.. ', end='')
            sys.stdout.flush()
            count_down -= 1
            count = 0

        candidates = []
        for pk in primary_keys:
            if len(pk[2]) != 1:
                continue
            implied_fk_name = pk[1] + pk[2][0]
            ratio = fuzz.ratio(col_name.lower(), implied_fk_name.lower())
            if ratio > 80:
                candidates.append((ratio, col[0], col[1], (col[2],), pk[0], pk[1], pk[2]))

        if len(candidates) > 0:
            candidates = sorted(candidates, key=lambda x: x[0], reverse=True)
            retval.append(candidates[0][1:])

    print('0')

    return retval


if __name__ == '__main__':
    config = parse_args()
    db = db_interface.get_db_interface(config)

    columns = db.get_columns() # [(schema, table, column), ...]
    primary_keys = db.get_primary_keys() # [(schema, table, [pk_columns, ...]), ...]
    foreign_keys = set(db.get_foreign_keys()) # [(fk_schema, fk_table, [fk_columns, ...], target_schema, target_table, [target_columns, ...]), ...]

    fk_by_heuristic = fk_name_heuristic(columns, primary_keys)
    # remove all foreign keys already reported
    new_fks  = [fk for fk in fk_by_heuristic if fk not in foreign_keys]

    width = 120
    if config.output:
        output_file = open(config.output, "w")
        width = 4000
        print('Writing discovered foreign keys to ' + config.output)
    else:
        output_file = sys.stdout
    pprint(new_fks, output_file, width=width)
    if config.output:
        output_file.close()

