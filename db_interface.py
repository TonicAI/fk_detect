import sys, os, psycopg2, mysql.connector

def get_db_interface(config):

    if not config.no_password:
        if 'FK_DETECT_PASSWORD' not in os.environ:
            print('please provide a DB password by setting FK_DETECT_PASSWORD environment variable, or setting --no-password')
            sys.exit(1)

        config.password = os.environ["FK_DETECT_PASSWORD"]

    if config.dbtype.lower() == 'mysql':
        return MySqlDbInterface(config)
    elif config.dbtype.lower() == 'postgres':
        return PostgresDbInterface(config)
    else:
        raise ValueError('database type must be "mysql" or "postgres"')

def tupleize(input):
    # goes two levels deep into a list, turning lists into tuples
    return [tuple([tuple(cell) if type(cell) is list else cell for cell in row]) for row in input]

class MySqlDbInterface:
    __sys_schema = ['information_schema', 'performance_schema', 'sys', 'mysql', 'innodb', 'tmp']
    __sys_schema_str = '(' + ', '.join(map(lambda s: "'" + s + "'", __sys_schema)) + ')'

    def __init__(self, config):
        if not config.no_password:
            self.__connection = mysql.connector.connect(user=config.user, password=config.password, host=config.host, port=config.port)
        else:
            self.__connection = mysql.connector.connect(user=config.user, host=config.host, port=config.port)

        self.__database = config.db if config.db else None

    def get_columns(self):
        cursor = self.__connection.cursor()
        try:
            cursor.execute("""SELECT table_schema, table_name, column_name
                                FROM information_schema.columns
                                WHERE table_schema NOT IN {} {}
                            """.format(self.__sys_schema_str, self.__database_clause('columns')))
            return cursor.fetchall()
        finally:
            cursor.close()

    def get_primary_keys(self):
        cursor = self.__connection.cursor()
        try:
            cursor.execute("""SELECT k.table_schema, k.table_name, group_concat(k.column_name)
                                FROM information_schema.table_constraints t
                                JOIN information_schema.key_column_usage k
                                    ON CAST(k.constraint_name AS BINARY) = CAST(t.constraint_name AS BINARY)
                                    AND CAST(k.table_schema AS BINARY) = CAST(t.table_schema AS BINARY)
                                    AND CAST(k.table_name AS BINARY) = CAST(t.table_name AS BINARY)
                                WHERE t.constraint_type='PRIMARY KEY' AND t.table_schema NOT IN {} {}
                                GROUP BY CAST(k.table_schema AS BINARY), CAST(k.table_name AS BINARY);
                            """.format(self.__sys_schema_str, self.__database_clause('k')))
            return tupleize([(r[0], r[1], r[2].split(',')) for r in cursor.fetchall()])
        finally:
            cursor.close()

    def get_foreign_keys(self):
        cursor = self.__connection.cursor()
        try:
            cursor.execute("""SELECT k.table_schema, k.table_name, group_concat(k.column_name ORDER BY k.ordinal_position), k.referenced_table_schema, k.referenced_table_name, group_concat(k.referenced_column_name)
                                FROM information_schema.table_constraints t
                                JOIN information_schema.key_column_usage k
                                  ON CAST(k.constraint_name AS BINARY) = CAST(t.constraint_name AS BINARY)
                                 AND CAST(k.table_schema AS BINARY) = CAST(t.table_schema AS BINARY)
                                 AND CAST(k.table_name AS BINARY) = CAST(t.table_name AS BINARY)
                                WHERE t.constraint_type='FOREIGN KEY' {}
                                GROUP BY 1, 2, 4, 5;""".format(self.__database_clause('k')))
            return tupleize([(r[0], r[1], r[2].split(','), r[3], r[4], r[5].split(',')) for r in cursor.fetchall()])
        finally:
            cursor.close()

    def __database_clause(self, tname):
        database_clause = ''
        if self.__database:
            database_clause = 'AND {}.table_schema IN (\'{}\')'.format(tname, self.__database)
        return database_clause


class PostgresDbInterface:

    __sys_schemas = ('pg_catalog', 'information_schema', 'pg_toast')

    def __init__(self, config):
        connection_string = 'dbname=\'{0}\' user=\'{1}\' host={2} port={3}'.format(config.db, config.user, config.host, config.port)
        if not config.no_password:
            connection_string = connection_string + ' password=\'{0}\''.format(config.password)
        if config.ssl:
            connection_string = connection_string + ' sslmode=required'

        self.__connection = psycopg2.connect(connection_string)

    def get_columns(self):
        with self.__connection.cursor() as cursor:
            cursor.execute("""SELECT ns.nspname, cl.relname, att.attname
                                FROM pg_attribute att
                                JOIN pg_class cl ON cl.oid = att.attrelid
                                JOIN pg_namespace ns ON ns.oid = cl.relnamespace
                                WHERE att.attnum > 0 AND ns.nspname NOT IN {0} AND cl.relkind = 'r'
                            """.format(self.__sys_schemas))
            return cursor.fetchall()

    def get_primary_keys(self):
        with self.__connection.cursor() as cursor:
            cursor.execute("""SELECT ns.nspname, cl.relname, array_agg(att.attname)
                                FROM pg_index ind
                                JOIN pg_class cl ON cl.oid = ind.indrelid
                                JOIN pg_namespace ns ON ns.oid = cl.relnamespace
                                JOIN pg_attribute att ON att.attrelid = cl.oid AND att.attnum = ANY(ind.indkey)
                                WHERE ns.nspname NOT IN {0} AND ind.indisprimary
                                GROUP BY 1, 2
                            """.format(self.__sys_schemas))

            return tupleize(cursor.fetchall())


    def get_foreign_keys(self):
        with self.__connection.cursor() as cursor:
            # ORDER BY in array_agg for tar_att is not a typo, fk_att.attnum is correct to keep relative ordering the same
            cursor.execute("""SELECT fk_nsp.nspname AS fk_schema, fk_table, array_agg(fk_att.attname ORDER BY fk_att.attnum), tar_nsp.nspname AS target_schema, target_table, array_agg(tar_att.attname ORDER BY fk_att.attnum)
                                FROM (
                                    SELECT
                                        fk.oid AS fk_table_id,
                                        fk.relnamespace AS fk_schema_id,
                                        fk.relname AS fk_table,
                                        unnest(con.conkey) as fk_column_id,

                                        tar.oid AS target_table_id,
                                        tar.relnamespace AS target_schema_id,
                                        tar.relname AS target_table,
                                        unnest(con.confkey) as target_column_id,

                                        con.connamespace AS constraint_nsp,
                                        con.conname AS constraint_name

                                    FROM pg_constraint con
                                    JOIN pg_class fk ON con.conrelid = fk.oid
                                    JOIN pg_class tar ON con.confrelid = tar.oid
                                    WHERE con.contype = 'f'
                                ) sub
                                JOIN pg_attribute fk_att ON fk_att.attrelid = fk_table_id AND fk_att.attnum = fk_column_id
                                JOIN pg_attribute tar_att ON tar_att.attrelid = target_table_id AND tar_att.attnum = target_column_id
                                JOIN pg_namespace fk_nsp ON fk_schema_id = fk_nsp.oid
                                JOIN pg_namespace tar_nsp ON target_schema_id = tar_nsp.oid
                                GROUP BY 1, 2, 4, 5, sub.constraint_nsp, sub.constraint_name;""")

            return tupleize(cursor.fetchall())

