from collections import MutableMapping
from pathlib import Path
import errno
import yaml


DEFAULT_CONF_DIR = Path('/etc/backuply')
DEFAULT_CONF_FILE = 'backuply.conf'


DEFAULT_SETTINGS = {}


class Settings(MutableMapping):
    def __init__(self, conf_dir=DEFAULT_CONF_DIR, conf_file=DEFAULT_CONF_FILE):
        if isinstance(conf_dir, str):
            self.conf_dir = Path(conf_dir)
        else:
            self.conf_dir = conf_dir
        if not self.conf_dir.exists():
            self.conf_dir.mkdir(0o755)

        self.conf_file = self.conf_dir.joinpath(conf_file)
        try:
            with self.conf_file.open('r') as f:
                self._raw_settings = yaml.safe_load(f)
                defaults = DEFAULT_SETTINGS.copy()
                defaults.update(self._raw_settings)
                self._raw_settings = defaults
        except IOError as e:
            if e.errno == errno.ENOENT:
                self._raw_settings = DEFAULT_SETTINGS
            else:
                raise e

    def __getitem__(self, item):
        return self._raw_settings[item]

    def __setitem__(self, key, value):
        self._raw_settings[key] = value

    def __iter__(self):
        return iter(self._raw_settings)

    def __len__(self):
        return len(self._raw_settings)

    def __delitem__(self, key):
        del self._raw_settings[key]

    def save(self):
        with self.conf_file.open('w') as f:
            yaml.dump(self._raw_settings, f)
