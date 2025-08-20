#!python
from archive.archive import Archive
from jupyter_aws.generic_templates import Arglist
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
    arc.restore()
elif cmd == "dir":
    listdir = args.shift("")
    arc.dir(listdir)
else:
    print(f"Unrecognized command '{cmd}'")
#arc.backup()
#arc.restore()

