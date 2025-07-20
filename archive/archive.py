from .archive_object import ArchiveObject
from .archive_data import ArchiveData
from .archive_config import ArchiveConfig
from .archive_entry import ArchiveEntry
from .archive_index import ArchiveIndex
import glob
import os
import sys

from math import log, log10, floor
def human_readable(value):
    suffix={
        3: "kB",
        6: "MB",
        9: "GB",
        12: "TB",
        15: "PB"
    }
    elevel = (floor(log10(value))//3*3)
    if elevel == 0:
        return f"{value}B"
    return f"{value / (10**elevel):.0f}{suffix[elevel]}"


class Archive:
    def __init__(self, archivename, debug):
        self.archivename = archivename
        self.config = ArchiveConfig(archivename, debug)
        self.objects = ArchiveObject(self.config)
        self.data = ArchiveData(self.config)
        self.index = ArchiveIndex(self.config, self.data)

    def backup(self):
        """Create a backup of the specified directory.

        This method scans the local mirror directory for files and uploads them to the archive.
        It creates an archive entry for each file, which includes its path, size, and hash.

        Side Effects:
            Creates archive entries for each file in the local mirror directory.
        """
        cfg = self.config
        obj = self.objects
        dat = self.data
        archive_id = dat.write_archive(self.archivename)
        for f in glob.glob(os.path.join(cfg.localmirror, "**"), recursive=True):
            if os.path.isfile(f):
                print(f)
                entry = obj.put_file(archive_id, f)
                dat.write_entry(archive_id, entry)

    def has_blob(self, hash):
        return self.index.has_hash(hash)

    def restore(self):
        """Restore the contents of the archive.

        This method retrieves all entries from the archive index and restores them to the local filesystem.

        Side Effects:
            Restores files to their original locations on the filesystem.
        """
        obj = self.objects
        archive_id = self.index.archive_id
        ihashes = self.index.ihashes
        entries = self.index.entries
        for e in entries:
            lpath = entries[e]['libpath']
            entry_id = entries[e]['id']
            hashes = ihashes[entry_id]
            print(lpath)
            entry = ArchiveEntry(archive_id, lpath, hashes, entries[e]['size'], entry_id)
            obj.get_file(entry)


    def dir(self, ldir):
        """List the contents of a directory in the archive.

        Args:
            ldir (str): The directory path to list.  If empty, lists the root directory.

        Side Effects:
            Prints the contents of the directory to standard output.
        """
        entryids, directories = self.index.idir(ldir)
        for id in entryids:
            e = self.index.entries[id]
            print(f"{human_readable(e['size']):>10s} {e['libpath']}")
        for lpath in directories:
            print(f"{'[dir]':>10s} {lpath}")
