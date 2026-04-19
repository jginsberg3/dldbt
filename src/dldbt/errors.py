class DldbtError(Exception):
    """Base class for all dldbt errors."""


class ConfigError(DldbtError):
    """Raised when the .dldbt.yml config is missing or invalid."""


class NotInitializedError(DldbtError):
    """Raised when a command needs `dldbt init` to have been run first."""


class BranchAlreadyExistsError(DldbtError):
    pass


class BranchNotFoundError(DldbtError):
    pass


class InvalidBranchNameError(DldbtError):
    pass
