#!python
from archive.archive import Archive
from generic_templates import Arglist
import sys

DEBUG=False
arc = Archive("openaudible", DEBUG)
args = Arglist(sys.argv)
app = args.shift()
cmd = args.shift()
args.shift_opts()
if cmd == "backup":
    arc.backup()
elif cmd == "restore":
    pattern = args.shift("*")
    print("pattern:", pattern)
    arc.restore(pattern)
elif cmd == "dir":
    listdir = args.shift("")
    arc.dir(listdir)
elif cmd == "find":
    search = args.shift("*")
    arc.find(search)
else:
    print(f"Unrecognized command '{cmd}'")
#arc.backup()
#arc.restore()

