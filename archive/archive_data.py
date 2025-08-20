import json
import sqlite3
import os
from .archive_config import ArchiveConfig
from .archive_entry import ArchiveEntry

class BucketedHashTable:
    def __init__(self):
        """Initialize the bucketed hash table.
        This class stores key-value pairs where each key can have a list of values.

        In the archive context, this is used to store the list of hashes associated with archive entries.
        """
        self.data = {}

    def add_item(self, key, value):
        if key in self.data:
            self.data[key].append(value)
        else:
            self.data[key] = [value]

    def keys(self):
        return self.data.keys()

    def items(self):
        return self.data.items()

    def __getitem__(self, key):
        return self.data[key]

class ArchiveData:
    def __init__(self, config : ArchiveConfig):
        """Initialize ArchiveData Database with the given configuration.

        This method sets up the SQLite database connection and initializes the schema if it does not exist.
        It also reads the schema version and upgrades the database if necessary.

        Args:
            config (ArchiveConfig): The configuration object for the archive.
        """
        self.config = config
        self.dbinit()

    def close(self):
        """Close the database connection."""
        self.con.close()

    def sql(self, _sql, *args, read=False):
        """Execute a SQL command on the database, returning results if read is True.
        Args:
            _sql (str): The SQL command to execute, with '?n' placeholders for parameters.
            *args: Parameters to bind to the SQL statement.
            read (bool): If True, fetches and returns results as a list of rows, each row being a dictionary mapping
                column names to values; otherwise, returns last row ID.
        """
        result = None
        cur = self.con.cursor()
        if self.config.debug:
            print("ArchiveData:", "sql()", _sql, args)
        cur.execute(_sql, args)
        if read:
            data = cur.fetchall()
            desc = cur.description
            result = [{
                desc[i][0]: data[j][i] for i in range( len(desc))
            } for j in range(len(data))]
        else:
            result = cur.lastrowid
        cur.close()
        return result

    def select(self, _sql, *args):
        """Runs a SELECT statement and returns the results as a list of dictionaries."""
        return self.sql(_sql, *args, read=True)

    def select1(self, _sql, *args, require=False):
        """Runs a SELECT statement and returns the first column of the first row as a single value.
        If no rows are returned, raises ValueError if require is True, otherwise returns None.
        """
        rows :dict = self.sql(_sql, *args, read=True)
        if len(rows) == 1:
            keys = list(rows[0].keys())
            if len(keys) == 1:
                return rows[0][keys[0]]
            else:
                raise ValueError(f"select1: Too many cols returned ({len(keys)}) from {_sql}, {args}")
        elif len(rows) > 1:
            raise ValueError(f"select1: Too many rows returned ({len(rows)}) from {_sql}, {args}")
        elif require:
            raise ValueError(f"select1: No matching rows for '{args}'")
        else:
            return None

    def sqlscript(self, script):
        """Execute a SQL script on the database.

        Args:
            script (str): The SQL script to execute, which can contain multiple SQL statements separated by semicolons.
        """
        cur = self.con.cursor()
        cur.executescript(script)
        cur.close()

    @property
    def schemaversion(self):
        """Get the current schema version from the SchemaVersion table.
        If the table does not exist, it initializes it with version 0."""
        try:
            ver = self.select("SELECT version FROM SchemaVersion")[0]['version']
        except sqlite3.OperationalError:
            ver = 0
            self.sql("CREATE TABLE IF NOT EXISTS SchemaVersion (version INTEGER)")
            self.sql("INSERT INTO SchemaVersion(version) VALUES (?)", ver)
        return ver

    @schemaversion.setter
    def schemaversion(self, ver : int):
        """Set the schema version in the SchemaVersion table.
        Args:
            ver (int): The new schema version to set.
        """
        sver = str(ver)
        cur = self.con.cursor()
        cur.execute("UPDATE SchemaVersion SET version=?", sver)
        cur.close()

    def dbupgrade(self, newver:int):
        """Upgrade the database schema to the specified version.

        Args:
            newver (int): The new schema version to upgrade to.
        """
        script = self.schema[newver]
        statements = script.strip().split(";")
        for statement in statements:
            self.sql(statement)
        #self.sqlscript(script)
        self.schemaversion = newver

    def dbinit(self):
        """Initialize the database schema.  The database is created if it does not exist, and the schema is upgraded to the target version.

        Raises:
            ValueError: If the configured database path is invalid or if the directory for the database cannot be created.
        """
        if not os.path.isfile(self.config.database):
            dbdir = os.path.dirname(self.config.database)
            os.makedirs(dbdir, exist_ok=True)
            if not os.path.isdir(dbdir):
                raise ValueError("Bad directory specified for database")

        # initialize database
        self.con = sqlite3.connect(self.config.database, autocommit=True)
        current_ver = self.schemaversion
        if self.config.debug:
            print("Current schema:",current_ver)
            print("Target schema", self.schema_target_ver)
        for v in range(self.schemaversion, self.schema_target_ver):
            self.dbupgrade(v+1)

    def write_archive(self, name):
        """Write a new archive with the given name to the database.
        Args:
            name (str): The name of the archive to create.
        """
        archive_id = self.sql("INSERT INTO Archive(name) VALUES (?)", name)
        return archive_id

    def del_entry(self, entry_id : int):
        """Delete an entry (an archived file) from the ArchiveEntry and EntryHashes tables by its ID.
        Args:
            entry_id (int): The ID of the entry to delete.
        """
        # todo: combine these into a single transaction
        self.sql("DELETE FROM ArchiveEntry WHERE id = ?", entry_id)
        self.sql("DELETE FROM EntryHashes WHERE archive_entry_id = ?", entry_id)

    def write_entry(self, entry : ArchiveEntry):
        """Upsert an entry (an archived file) to the database.

        Args:
            entry (ArchiveEntry): The entry to write.

        Returns:
            ArchiveEntry: The written entry, including its ID.
        """
        #self.del_entry()
        if entry.id >= 0:
            self.sql("UPDATE ArchiveEntry(archive_id, libpath, size) VALUES (?, ?, ?) WHERE id = ?",
                     entry.archive_id, entry.libpath, entry.size, entry.id)
        else:
            entry.id = self.sql("INSERT INTO ArchiveEntry(archive_id, libpath, size) VALUES (?,?,?)",
                                entry.archive_id, entry.libpath, entry.size)
        #print("entry=",entry_id)
        for i,h in enumerate(entry.hashlist):
            #print(h)
            self.sql("INSERT INTO EntryHashes(archive_id, archive_entry_id, seq, hash) VALUES (?,?,?,?)",
                     entry.archive_id, entry.id, i, h)
        return entry

    def read_entries(self, archive_name):
        """Read entries from the archive with the given name.
        This method retrieves the archive ID, entry hashes, and index of entries for the specified archive name.

        Args:
            archive_name (str): The name of the archive to read entries from.

        Returns:
            tuple(archive_id, ihashes, index): A tuple containing the archive ID, a BucketedHashTable of entry hashes, and an index of entries.
                   If the archive does not exist, returns None.
        """
        archive_id = self.select1("SELECT id FROM Archive WHERE name = ?", archive_name)
        if archive_id is None:
            return None

        entries = self.select("SELECT id, libpath, size FROM ArchiveEntry WHERE archive_id = ?", archive_id)
        hashes = self.select("SELECT archive_entry_id, seq, hash FROM EntryHashes WHERE archive_id = ? ORDER BY archive_entry_id, seq",
                             archive_id)
        ihashes=BucketedHashTable()
        for row in hashes:
            eid = row['archive_entry_id']
            ihashes.add_item(eid, row['hash'])
            assert(len(ihashes.data[eid])==row['seq']+1)
        index = {}
        for row in entries:
            eid = row['id']
            index[eid] = row
        return archive_id, ihashes, index


    # Schema versions and DDL scripts to initialize and upgrade the database schema.  Note that the
    # schema version is stored in the SchemaVersion table starting at version 0 with no other tables,
    # so the first usable schema version is 1.
    schema_target_ver = 1
    schema = {
        1: """
CREATE TABLE IF NOT EXISTS Archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT
);
CREATE TABLE IF NOT EXISTS ArchiveEntry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    archive_id INTEGER,
    libpath TEXT,
    size INTEGER,
    FOREIGN KEY(archive_id) REFERENCES Archive (id)
);
CREATE TABLE IF NOT EXISTS EntryHashes (
    archive_id INTEGER,
    archive_entry_id INTEGER,
    seq INTEGER,
    hash TEXT,
    FOREIGN KEY(archive_id) REFERENCES Archive (id),
    FOREIGN KEY(archive_entry_id) REFERENCES ArchiveEntry(id)
);
--CREATE UNIQUE INDEX (archive_entry, order) on EntryHashes;
--CREATE UNIQUE INDEX (archive) on ArchiveEntry;
"""
}
