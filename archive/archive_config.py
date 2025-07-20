import socket
import yaml
CONFIG_PATH="config.yaml"

class ArchiveConfig:
    def __init__(self, name, debug=False):
        if debug:
            print("ArchiveConfig:", name)
        self.debug = debug
        self.archive_name = name
        self.hostname = socket.gethostname()
        with open(CONFIG_PATH, "rt") as f:
            self.config = yaml.load(f, yaml.loader.SafeLoader)['archive'][name]

    @property
    def database(self):
        return self.config['index']['database']

    @property
    def verifyreads(self):
        return self.config['verifyreads']

    @property
    def localmirror(self):
        return self.config['mirrors'][self.hostname]

    @property
    def objectstore(self):
        return self.config