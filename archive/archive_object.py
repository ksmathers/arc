import jupyter_aws as jaws
from hashlib import md5
import base64
import os
import sys
from .archive_config import ArchiveConfig
from .archive_entry import ArchiveEntry

def KiB(n): return n*1024
def MiB(n): return KiB(n)*1024
def GiB(n): return MiB(n)*1024


CONFIG="~/.archive.json"
LIBRARY="library"
MAXBLOB=MiB(32)


class ArchiveObject:
    def __init__(self, config : ArchiveConfig):
        """Initialize the ArchiveObject storage container for managing files (and their component blobs) in an object store.
        This class uses the 'multicloud' store context to handle file storage and retrieval.

        Args:
            config (ArchiveConfig): The configuration object for the archive.
        """
        self.config = config
        if self.config.debug:
            print("ArchiveObject:objectstore:",config.objectstore)
        self.backend = jaws.Context('objectstore', config.objectstore) # todo: replace with multicloud context
        self.verify_reads = self.config.verifyreads

    @classmethod
    def hashpath(cls, hash):
        path = f"{hash[0:2]}/{hash[2:4]}/{hash[4:6]}/{hash[6:8]}/{hash}"
        return path

    def put_blob(self, blob : bytes) -> str:
        """
        Stores a blob of data in the object store and return its hash.  If the blob already exists, it rewrites the existing blob.

        Args:
            blob (bytes): The blob of data to store.

        Returns:
            str: The hash of the stored blob.
        """
        hash = base64.b16encode(md5(blob).digest()).decode('ASCII')
        o = self.backend.object(self.hashpath(hash))
        o.put_bytes(blob)
        return hash

    def get_blob(self, hash : str):
        o = self.backend.object(self.hashpath(hash))
        buf = o.get_bytes()
        if self.verify_reads:
            success = (base64.b16encode(md5(buf)) == hash)
            assert(success)
        return buf

    def put_file(self, archive_id, path) -> ArchiveEntry:
        basedir = os.path.join(self.config.localmirror, ".")[:-1]
        assert(path.startswith(basedir))
        libpath = path.replace(basedir, "").replace("\\", "/") #normalize path
        hashlist = []
        with open(path, "rb") as f:
            f_size = os.fstat(f.fileno()).st_size
            for begin in range(0, f_size, MAXBLOB):
                buf = f.read(MAXBLOB)
                h = self.put_blob(buf)
                hashlist.append(h)
        return ArchiveEntry(archive_id, libpath, hashlist, f_size)

    def get_file(self, library_entry : ArchiveEntry):
        basedir = os.path.join(self.config.localmirror, ".")[:-1]
        path = library_entry.libpath
        if sys.platform.startswith("win"):
            path = path.replace("/", "\\")
        path = os.path.join(basedir, path)
        total_size = 0
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            nparts = (library_entry.size // MAXBLOB)+1
            for part in range(0, nparts):
                buf = self.get_blob(library_entry.hashlist[part])
                if part < nparts-1:
                    # catch out of order parts, shouldn't happen
                    assert(len(buf) == MAXBLOB)
                f.write(buf)
                total_size += len(buf)
        assert(library_entry.size == total_size)





