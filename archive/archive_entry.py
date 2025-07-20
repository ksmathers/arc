from typing import List

class ArchiveEntry:
    seq_id = 0
    def __init__(self, archive_id : int, libpath : str, hashlist : List[str], size : int, id : int = None):
        self.archive_id = archive_id
        self.id = id if id is not None else self.next_seq_id()
        self.libpath : str = libpath
        self.hashlist : List[str] = hashlist
        self.size : int = size

    @classmethod
    def next_seq_id(cls):
        cls.seq_id -= 1
        return cls.seq_id

    def serialize(self):
        return [
            { "libpath":self.libpath, "part":i, "hash":self.hashlist[i], "size":self.size }
                for i in range(len(self.hashlist))
        ]
