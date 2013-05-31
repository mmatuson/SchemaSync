from utils import REGEX_TABLE_AUTO_INC, REGEX_TABLE_COMMENT


def sync_schema(fromdb, todb, options):
    """Generate the SQL statements needed to sync two Databases and all of
       their children (Tables, Columns, Indexes, Foreign Keys)

    Args:
        fromdb: A SchemaObject Schema Instance.
        todb: A SchemaObject Schema Instance.
        options: dictionary of options to use when syncing schemas
            sync_auto_inc: Bool, sync auto inc value throughout the schema?
            sync_comments: Bool, sync comment fields trhoughout the schema?

    Yields:
        A tuple (patch, revert) containing the next SQL statement needed
        to migrate fromdb to todb. The tuple will always contain 2 strings,
        even if they are empty.
    """

    p, r = sync_database_options(fromdb, todb)
    if p and r:
        yield ("%s %s;" % (todb.alter(), p),
               "%s %s;" % (todb.alter(), r))

    for p, r in sync_created_tables(fromdb.tables, todb.tables,
                                   sync_auto_inc=options['sync_auto_inc'],
                                   sync_comments=options['sync_comments']):
        yield p, r

    for p, r in sync_dropped_tables(fromdb.tables, todb.tables,
                                    sync_auto_inc=options['sync_auto_inc'],
                                    sync_comments=options['sync_comments']):
        yield  p, r

    for t in fromdb.tables:
        if not t in todb.tables:
            continue

        from_table = fromdb.tables[t]
        to_table = todb.tables[t]

        plist = []
        rlist = []
        for p, r in sync_table(from_table, to_table, options):
			pAlter = "%s %s;"% (to_table.alter(),p)
			rAlter = "%s %s;"% (to_table.alter(),r)
            yield pAlter,rAlter


def sync_table(from_table, to_table, options):
    """Generate the SQL statements needed to sync two Tables and all of their
       children (Columns, Indexes, Foreign Keys)

    Args:
        from_table: A SchemaObject TableSchema Instance.
        to_table: A SchemaObject TableSchema Instance.
        options: dictionary of options to use when syncing schemas
            sync_auto_inc: Bool, sync auto inc value throughout the table?
            sync_comments: Bool, sync comment fields trhoughout the table?

    Yields:
        A tuple (patch, revert) containing the next SQL statements
    """
    for p, r in sync_created_columns(from_table.columns,
                                     to_table.columns,
                                     sync_comments=options['sync_comments']):
        yield (p, r)

    for p, r in sync_dropped_columns(from_table.columns,
                                    to_table.columns,
                                    sync_comments=options['sync_comments']):
        yield (p, r)

    if from_table and to_table:
        for p, r in sync_modified_columns(from_table.columns,
                                          to_table.columns,
                                          sync_comments=options['sync_comments']):
            yield (p, r)

        # add new indexes, then compare existing indexes for changes
        for p, r in sync_created_constraints(from_table.indexes, to_table.indexes):
            yield (p, r)

        for p, r in sync_modified_constraints(from_table.indexes, to_table.indexes):
            yield (p, r)

        # we'll drop indexes after we process foreign keys...

        # add new foreign keys and compare existing fks for changes
        for p, r in sync_created_constraints(from_table.foreign_keys,
                                             to_table.foreign_keys):
            yield (p, r)

        for p, r in sync_modified_constraints(from_table.foreign_keys,
                                              to_table.foreign_keys):
            yield (p, r)

        for p, r in sync_dropped_constraints(from_table.foreign_keys,
                                             to_table.foreign_keys):
            yield (p, r)

        #drop remaining indexes
        for p, r in sync_dropped_constraints(from_table.indexes, to_table.indexes):
            yield (p, r)

        # end the alter table syntax with the changed table options
        p, r = sync_table_options(from_table, to_table,
                                 sync_auto_inc=options['sync_auto_inc'],
                                 sync_comments=options['sync_comments'])
        if p:
            yield (p, r)


def sync_database_options(from_db, to_db):
    """Generate the SQL statements needed to modify the Database options
       of the target schema (patch), and restore them to their previous
       definition (revert)

    Args:
        from_db: A SchemaObject DatabaseSchema Instance.
        to_db: A SchemaObject DatabaseSchema Instance.
        options: dictionary of options to use when syncing schemas
            sync_auto_inc: Bool, sync auto increment value throughout the table?
            sync_comments: Bool, sync comment fields trhoughout the table?

    Returns:
        A tuple (patch, revert) containing the SQL statements
        A tuple of empty strings will be returned if no changes were found
    """
    p = []
    r = []

    for opt in from_db.options:
        if from_db.options[opt] != to_db.options[opt]:
            p.append(from_db.options[opt].create())
            r.append(to_db.options[opt].create())

    if p:
        return (' '.join(p), ' '.join(r))
    else:
        return ('', '')


def sync_created_tables(from_tables, to_tables,
                        sync_auto_inc=False, sync_comments=False):
    """Generate the SQL statements needed to CREATE Tables in the target
       schema (patch), and remove them (revert)

    Args:
        from_tables: A OrderedDict of SchemaObject.TableSchema Instances.
        to_tables: A OrderedDict of SchemaObject.TableSchema Instances.
        sync_auto_inc: Bool (default=False), sync auto increment for each table?
        sync_comments: Bool (default=False), sync the comment field for the table?

    Yields:
        A tuple (patch, revert) containing the next SQL statements
    """
    for t in from_tables:
        if t not in to_tables:
            p, r = from_tables[t].create(), from_tables[t].drop()
            if not sync_auto_inc:
                p = REGEX_TABLE_AUTO_INC.sub('', p)
                r = REGEX_TABLE_AUTO_INC.sub('', r)
            if not sync_comments:
                p = REGEX_TABLE_COMMENT.sub('', p)
                r = REGEX_TABLE_COMMENT.sub('', r)

            yield p, r


def sync_dropped_tables(from_tables, to_tables,
                        sync_auto_inc=False, sync_comments=False):
    """Generate the SQL statements needed to DROP Tables in the target
       schema (patch), and restore them to their previous definition (revert)

    Args:
        from_tables: A OrderedDict of SchemaObject.TableSchema Instances.
        to_tables: A OrderedDict of SchemaObject.TableSchema Instances.
        sync_auto_inc: Bool (default=False), sync auto increment for each table?
        sync_comments: Bool (default=False), sync the comment field for the table?

    Yields:
        A tuple (patch, revert) containing the next SQL statements
    """
    for t in to_tables:
        if t not in from_tables:
            p, r = to_tables[t].drop(), to_tables[t].create()
            if not sync_auto_inc:
                p = REGEX_TABLE_AUTO_INC.sub('', p)
                r = REGEX_TABLE_AUTO_INC.sub('', r)
            if not sync_comments:
                p = REGEX_TABLE_COMMENT.sub('', p)
                r = REGEX_TABLE_COMMENT.sub('', r)

            yield p, r


def sync_table_options(from_table, to_table,
                       sync_auto_inc=False, sync_comments=False):
    """Generate the SQL statements needed to modify the Table options
       of the target table (patch), and restore them to their previous
       definition (revert)

    Args:
       from_table: A SchemaObject TableSchema Instance.
       to_table: A SchemaObject TableSchema Instance.
       sync_auto_inc: Bool, sync the tables auto increment value?
       sync_comments: Bool, sync the tbales comment field?

    Returns:
       A tuple (patch, revert) containing the SQL statements.
       A tuple of empty strings will be returned if no changes were found
    """
    p = []
    r = []

    for opt in from_table.options:
        if ((opt == 'auto_increment' and not sync_auto_inc) or
            (opt == 'comment' and not sync_comments)):
            continue

        if from_table.options[opt] != to_table.options[opt]:
            p.append(from_table.options[opt].create())
            r.append(to_table.options[opt].create())

    if p:
        return (' '.join(p), ' '.join(r))
    else:
        return ('', '')


def get_previous_item(lst, item):
    """ Given an item, find its previous item in the list
        If the item appears more than once in the list, return the first index

        Args:
            lst: the list to search
            item: the item we want to find the previous item for

        Returns: The previous item or None if not found.
    """
    try:
        i = lst.index(item)
        if i > 0:
            return lst[i - 1]
    except (IndexError, ValueError):
        pass

    return None


def sync_created_columns(from_cols, to_cols, sync_comments=False):
    """Generate the SQL statements needed to ADD Columns to the target
       table (patch) and remove them (revert)

    Args:
        from_cols: A OrderedDict of SchemaObject.ColumnSchema Instances.
        to_cols: A OrderedDict of SchemaObject.ColumnSchema Instances.
        sync_comments: Bool (default=False), sync the comment field for each column?

    Yields:
        A tuple (patch, revert) containing the next SQL statements
    """
    for c in from_cols:
        if c not in to_cols:
            fprev = get_previous_item(from_cols.keys(), c)
            yield (from_cols[c].create(after=fprev, with_comment=sync_comments),
                   from_cols[c].drop())


def sync_dropped_columns(from_cols, to_cols, sync_comments=False):
    """Generate the SQL statements needed to DROP Columns in the target
       table (patch) and restore them to their previous definition (revert)

    Args:
        from_cols: A OrderedDictionary of SchemaObject.ColumnSchema Instances.
        to_cols: A OrderedDictionary of SchemaObject.ColumnSchema Instances.
        sync_comments: Bool (default=False), sync the comment field for each column?

    Yields:
        A tuple (patch, revert) containing the next SQL statements
    """
    for c in to_cols:
        if c not in from_cols:
            tprev = get_previous_item(to_cols.keys(), c)
            yield (to_cols[c].drop(),
                   to_cols[c].create(after=tprev, with_comment=sync_comments))


def sync_modified_columns(from_cols, to_cols, sync_comments=False):
    """Generate the SQL statements needed to MODIFY Columns in the target
       table (patch) and restore them to their previous definition (revert)

    Args:
        from_cols: A OrderedDict of SchemaObject.ColumnSchema Instances.
        to_cols: A OrderedDict of SchemaObject.ColumnSchema Instances.
        sync_comments: Bool (default=False), sync the comment field for each column?

    Yields:
        A tuple (patch, revert) containing the next SQL statements
    """
    # find the column names comomon to each table
    # and retain the order in which they appear
    from_names = [c for c in from_cols.keys() if c in to_cols]
    to_names = [c for c in to_cols.keys() if c in from_cols]

    for from_idx, name in enumerate(from_names):

        to_idx = to_names.index(name)

        if ((from_idx != to_idx) or
            (to_cols[name] != from_cols[name]) or
            (sync_comments and (from_cols[name].comment != to_cols[name].comment))):

            # move the element to its correct spot as we do comparisons
            # this will prevent a domino effect of off-by-one false positives.
            if from_names.index(to_names[from_idx]) > to_idx:
                name = to_names[from_idx]
                from_names.remove(name)
                from_names.insert(from_idx, name)
            else:
                to_names.remove(name)
                to_names.insert(from_idx, name)

            fprev = get_previous_item(from_cols.keys(), name)
            tprev = get_previous_item(to_cols.keys(), name)
            yield (from_cols[name].modify(after=fprev, with_comment=sync_comments),
                   to_cols[name].modify(after=tprev, with_comment=sync_comments))


def sync_created_constraints(src, dest):
    """Generate the SQL statements needed to ADD constraints
       (indexes, foreign keys) to the target table (patch)
       and remove them (revert)

    Args:
        src: A OrderedDictionary of SchemaObject IndexSchema
             or ForeignKeySchema Instances
        dest: A OrderedDictionary of SchemaObject IndexSchema
              or ForeignKeySchema Instances

    Yields:
        A tuple (patch, revert) containing the next SQL statements
    """
    for c in src:
        if c not in dest:
            yield src[c].create(), src[c].drop()


def sync_dropped_constraints(src, dest):
    """Generate the SQL statements needed to DROP constraints
       (indexes, foreign keys) from the target table (patch)
       and re-add them (revert)

    Args:
        src: A OrderedDict of SchemaObject IndexSchema
             or ForeignKeySchema Instances
        dest: A OrderedDict of SchemaObject IndexSchema
              or ForeignKeySchema Instances

    Yields:
        A tuple (patch, revert) containing the next SQL statements
    """
    for c in dest:
        if c not in src:
            yield dest[c].drop(), dest[c].create()


def sync_modified_constraints(src, dest):
    """Generate the SQL statements needed to modify
       constraints (indexes, foreign keys) in the target table (patch)
       and restore them to their previous definition (revert)

       2 tuples will be generated for every change needed.
       Constraints must be dropped and re-added, since you can not modify them.

    Args:
        src: A OrderedDict of SchemaObject IndexSchema
             or ForeignKeySchema Instances
        dest: A OrderedDict of SchemaObject IndexSchema
              or ForeignKeySchema Instances

    Yields:
        A tuple (patch, revert) containing the next SQL statements
    """
    for c in src:
        if c in dest and src[c] != dest[c]:
            yield dest[c].drop(), dest[c].drop()
            yield src[c].create(), dest[c].create()
