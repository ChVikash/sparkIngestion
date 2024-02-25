"""
Module to store any custom exceptions
"""


class IllegalTablePathException(Exception):
    """
    Exception raised for errors in the table path.
    """

    def __init__(self, message="Table Path Incorrect"):
        super().__init__(message)
        
class IncorrectLocationException(Exception):
    """
    Exception raised for errors in the table path.
    """

    def __init__(self, message="Table Path Incorrect"):
        super().__init__(message)


class ColumnDroppedException(Exception):
    """
    Exception raised when an existing column(s) stops coming in from source.
    """
    def __init__(self, message="""A Column(s) has been dropped from the table. Please make {OverwriteSchema
    :True} in the params of the table config"""):
        super().__init__(message)

class ColumnAddedException(Exception):
    """
    Exception raised when a new column(s) stops coming in from source.
    """
    def __init__(self, message="""A Column(s) has been added to the table. Please make {OverwriteSchema
    :True} in the params of the table config"""):
        super().__init__(message)

class ColumnMismatchException(Exception):
    """
    Exception raised when a new column(s) stops coming in from source.
    """
    def __init__(self, col_name):
        self.message=f"""A Column(s) {col_name} is changed in the table. Please make [OverwriteSchema
    :True] in the params of the table config"""
        super().__init__(self.message)

class CountMismatchException(Exception):
    """
    Exception raised when there is a count mismatch between pre load and post load of ingestion.
    """
    def __init__(self, cnt):
        self.message=f"""There is a count differnce (pre_load_cnt - post_load_cnt = {cnt})"""
        super().__init__(self.message)

class NoConfigFoundError(Exception):
    """
    Error raised when there is no config present at the given path.
    """
    def __init__(self, path):
        self.message=f"""No Souce config found at {path})"""
        super().__init__(self.message)


class DbNotFoundException(Exception):
    """
    Error raised when there is no config present at the given path.
    """
    def __init__(self, catalog):
        self.message=f"""No databases found in catalog {catalog})"""
        super().__init__(self.message)