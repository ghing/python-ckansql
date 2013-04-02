import exceptions
import datetime
import urllib
import time
import requests

# TODO: Figure out appropriate value for this
apilevel = "2.0"

# TODO: Is this thread safe? I'm not really sure.  For now assume it's not
threadsafety = 0

# Python extended format codes, e.g. ...WHERE name=%(name)s
paramstyle = "pyformat"

# Exceptions
class Warning(exceptions.StandardError):
    pass

class Error(exceptions.StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class DataError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class InternalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass

# Type objects and constructors

def Date(year, month, day):
    return datetime.date(year, month, day)

def Time(hour, minute, second):
    return datetime.time(hour, minute, second)

def Timestamp(year, month, day, hour, minute, second):
    return datetime.datetime(year, month, day, hour, minute, second)

def DateFromTicks(ticks):
    return Date(*time.localtime(ticks)[:3])

def TimeFromTicks(ticks):
    return Time(*time.localtime(ticks)[3:6])

def TimestampFromTicks(ticks):
    return Timestamp(*time.localtime(ticks)[:6])


class DBAPITypeObject:
    def __init__(self,*values):
        self.values = values
    def __cmp__(self,other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1


TYPE_CODES = {
    'STRING': 1,
    'BINARY': 2,
    'NUMBER': 3,
    'DATETIME': 4,
    'ROWID': 5,
}


STRING = DBAPITypeObject(TYPE_CODES['STRING'])
BINARY = DBAPITypeObject(TYPE_CODES['BINARY'])
NUMBER = DBAPITypeObject(TYPE_CODES['NUMBER'])
DATETIME = DBAPITypeObject(TYPE_CODES['DATETIME'])
ROWID = DBAPITypeObject(TYPE_CODES['ROWID'])


class Cursor(object):
    def __init__(self, conn):
        self.arraysize = 1
        self._conn = conn
        self._rows = []
        self._row_index = 0
        self._cols = {}
        self._closed = False
        self.rowcount = -1

    def _rows_remaining(self):
        remaining = len(self._rows) - self._row_index
        if remaining < 0:
            remaining = 0
        return remaining

    def _update_row_index(self, count):
        self._row_index += count

    def _row_to_seq(self, row):
        """Convert a row dictionary to a seq"""
        return [row[col] for col in self._cols]

    def callproc(self, procname, parameters=None):
        raise NotSupportedError

    def close(self):
        self._rows = []
        self._row_index = 0
        self._cols = {}
        self._closed = True

    def execute(self, operation, parameters=None):
        query = {}
        if parameters is not None:
            operation = operation % parameters
        query[self._conn.query_param] = operation
        url = self._conn.url + '?' + urllib.urlencode(query)
        r = requests.get(url)
        self._rows = r.json()
        self._row_index = 0
        # Get the column names and ordering from the first
        # row. This is a workaround for the fact that
        # dictionaries aren't ordered in Python
        numrows = len(self._rows)
        if numrows:
            self.rowcount = numrows
            self._cols = self._rows[0].keys()
            # For now, interpret everything as a string
            self.description = [(col, TYPE_CODES['STRING']) for col in self._cols]

    def executemany(self, operation, seq_of_parameters):
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def fetchone(self):
        if not self._rows_remaining():
            return None

        row = self._rows[self._row_index]
        self._update_row_index(1)
        return self._row_to_seq(row)

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        remaining = self._rows_remaining()
        if remaining < size:
            size = remaining
        
        end = self._row_index + size - 1
        rows = self._rows[self._row_index:end]
        self._update_row_index(size)

        return [self._row_to_seq(row) for row in rows]

    def fetchall(self):
        if not self._rows_remaining:
            return []

        rows = self._rows[self._row_index:]
        self._row_index = len(self._rows) + 1
        return [self._row_to_seq(row) for row in rows]

    def nextset(self):
        raise NotSupportedError

    def setinputsizes(self, sizes):
        # Do nothing
        pass

    def setoutputsize(self, size, column=None):
        # Do nothing
        pass


class Connection(object):
    def __init__(self, url, query_param='q'):
        self.url = url
        self.query_param = query_param

    def close(self):
        # TODO: Implement this
        pass

    def commit(self):
        # Do nothing. This is read-only, so obviously there isn't
        # transaction support
        pass

    def rollback(self):
        # Not implemented. This is read-only, so obviously there isn't
        # transaction support
        raise NotSupportedError

    def cursor(self):
        return Cursor(self)


def connect(url):
    return Connection(url)


__all__ = filter(lambda k: not k.startswith('_'), locals().keys())
