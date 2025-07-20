from .archive_entry import ArchiveEntry
from .archive_data import ArchiveData
from .archive_config import ArchiveConfig
from typing import List

class ArchiveIndex:
    def __init__(self, config : ArchiveConfig, adata : ArchiveData):

        t = adata.read_entries(config.archive_name)
        if t:
            archive_id, ihashes, index = t
            if config.debug:
                print(f"ArchiveIndex: {config.archive_name} loaded {len(index)} entries and {len(ihashes.keys())} hashes")
            self.archive_id = archive_id   # identifier for the archive
            self.ihashes = ihashes  # entry_id -> [hashes]
            self.entries = index  # entry_id -> { 'id', 'libpath', 'size' }
            self.all_hashes = set()
            for _key,hlist in self.ihashes.items():
                for h in hlist:
                    self.all_hashes.add(h)
            # self.byname = { i['libpath'] : i['id'] for i in index }
        else:
            if config.debug:
                print("ArchiveIndex:", config.archive_name)
            self.archive_id = -1
            self.ihashes = {}
            self.entries = {}
            self.all_hashes = set()
            # self.byname = {}

    def hashes(self, libpath : str) -> List[str]:
        return self.libpath[libpath]

    def has_hash(self, hash : str) -> bool:
        return hash in self.all_hashes

    def idir(self, prefix : str) -> List[int]:
        if len(prefix) > 0 and not prefix.endswith('/'):
            prefix += '/'
        nsep = prefix.count('/')

        entries = []
        directories = []
        for i,e in self.entries.items():
            lpath :str = e['libpath']
            if lpath.startswith(prefix):
                if lpath.count('/')==nsep:
                    entries.append(e['id'])
                else:
                    ldir = lpath[0:lpath.find('/', len(prefix))]
                    if not ldir in directories:
                        directories.append(ldir)
        return entries, directories







