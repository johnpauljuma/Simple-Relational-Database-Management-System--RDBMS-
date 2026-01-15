"""
Custom exception classes for MyRDBMS
"""

class MyRDBMSError(Exception):
    """Base exception for MyRDBMS"""
    pass

class ParseError(MyRDBMSError):
    """SQL parsing error"""
    pass

class ExecutionError(MyRDBMSError):
    """Query execution error"""
    pass

class StorageError(MyRDBMSError):
    """Storage engine error"""
    pass

class SchemaError(MyRDBMSError):
    """Schema validation error"""
    pass

class ConstraintError(MyRDBMSError):
    """Constraint violation error"""
    pass

class TableNotFoundError(MyRDBMSError):
    """Table does not exist"""
    pass

class DatabaseNotFoundError(MyRDBMSError):
    """Database does not exist"""
    pass

class IndexError(MyRDBMSError):
    """Index-related error"""
    pass

class JoinError(MyRDBMSError):
    """Join operation error"""
    pass

class TransactionError(MyRDBMSError):
    """Transaction-related error"""
    pass