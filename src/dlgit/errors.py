class DlgitError(Exception):
    """Base class for all dlgit errors."""


class ConfigError(DlgitError):
    """Raised when the .dlgit.yml config is missing or invalid."""


class NotInitializedError(DlgitError):
    """Raised when a command needs `dlgit init` to have been run first."""


class BranchAlreadyExistsError(DlgitError):
    pass


class BranchNotFoundError(DlgitError):
    pass


class InvalidBranchNameError(DlgitError):
    pass
