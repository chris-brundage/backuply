class InvalidBackupTarget(IOError):
    def __init__(self, errno, message, backup_target=None, backup_type=None, *args, **kwargs):
        """

        :param message:
        :param backup_target:
        :param backup_type:
        """
        super().__init__(errno, message, *args)
        self.backup_type = backup_type
        self.backup_target = backup_target

    def __str__(self):
        out_str = 'Backup target {} is not valid for the type {}. The error was "{}"'
        out_str = out_str.format(self.backup_target, self.backup_type, self.strerror)

        return out_str


class InvalidConfigurationError(Exception):
    pass
