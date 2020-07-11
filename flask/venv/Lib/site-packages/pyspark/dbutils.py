from collections import namedtuple

from py4j.protocol import Py4JJavaError
from six import string_types

from pyspark.context import SparkContext


class DBUtils(object):
    def __init__(self, sc=None):
        if sc is None:
            sc = SparkContext.getOrCreate()
        self.fs = FSHandler(sc._jvm.com.databricks.service.DBUtils.fs())
        self.secrets = SecretsHandler(sc._jvm.com.databricks.service.DBUtils.secrets())


class FileInfo(namedtuple('FileInfo', ['path', 'name', 'size'])):

    def isDir(self):
        return self.name.endswith('/')

    def isFile(self):
        return not self.isDir()

    @staticmethod
    def create_from_jschema(j_file_info):
        return FileInfo(path=j_file_info.path(), name=j_file_info.name(), size=j_file_info.size())


class FSHandler:

    def __init__(self, fsutils):
        self.fsutils = fsutils

    def __call__(self):
        return self

    def print_return(self, result):
        return result

    def check_types(self, vars_and_types):
        for (var, type) in vars_and_types:
            if not isinstance(var, type):
                raise TypeError(u'{0} has the wrong type - {1} is expected.'
                                .format(repr(var), type))

    def create_list_from_jschema(self, jschema, create_obj_from_jschema):
        return [create_obj_from_jschema(jschema.apply(i)) for i in range(jschema.length())]

    def prettify_exception_message(f):
        """
        This is a decorator function that aims to properly display errors that happened on the
        Scala side. Without such handling, stack traces from Scala are displayed at the
        bottom of error output, and are easily missed. We fix this by catching Py4JJavaError
        and throwing another exception with the error message from Scala side.
        """
        def f_with_exception_handling(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Py4JJavaError as e:
                class ExecutionError(Exception):
                    pass
                # In Python 3, we need to use the new 'raise X from None' syntax
                # to suppress the original exception's traceback. However, we
                # can't directly use that syntax because we need to be compatible
                # with Python 2. It might appear that six's `raise_from` would
                # handle this but that function's implementation is wrong and the
                # six won't fix it: https://github.com/benjaminp/six/issues/193.
                # Therefore, we need this gross hack derived from PEP-409:
                exc = ExecutionError(str(e))
                exc.__context__ = None
                exc.__cause__ = None
                raise exc
        return f_with_exception_handling

    # fsutils functions

    @prettify_exception_message
    def cp(self, source, dest, recurse=False):
        self.check_types([(source, string_types), (dest, string_types), (recurse, bool)])
        return self.print_return(self.fsutils.cp(source, dest, recurse))

    @prettify_exception_message
    def head(self, file, max_bytes=65536):
        self.check_types([(file, string_types), (max_bytes, int)])
        return self.print_return(self.fsutils.head(file, max_bytes))

    @prettify_exception_message
    def ls(self, path):
        self.check_types([(path, string_types)])
        return self.create_list_from_jschema(self.print_return(self.fsutils.ls(path)),
                                             FileInfo.create_from_jschema)

    @prettify_exception_message
    def mkdirs(self, dir):
        self.check_types([(dir, string_types)])
        return self.print_return(self.fsutils.mkdirs(dir))

    @prettify_exception_message
    def mv(self, source, dest, recurse=False):
        self.check_types([(source, string_types), (dest, string_types), (recurse, bool)])
        return self.print_return(self.fsutils.mv(source, dest, recurse))

    @prettify_exception_message
    def put(self, file, contents, overwrite=False):
        self.check_types([(file, string_types), (contents, string_types), (overwrite, bool)])
        return self.print_return(self.fsutils.put(file, contents, overwrite))

    @prettify_exception_message
    def rm(self, dir, recurse=False):
        self.check_types([(dir, string_types), (recurse, bool)])
        return self.print_return(self.fsutils.rm(dir, recurse))

    def __repr__(self):
        # TODO(tjh) call the scala doc
        return "Package 'dbutils.fs'. For more information, type 'display(dbutils.fs)'" \
               " in a cell."


class SecretScope(namedtuple('SecretScope', ['name'])):

    def getName(self):
        return self.name

    @staticmethod
    def create_from_jschema(j_secret_scope):
        return SecretScope(name=j_secret_scope.name())


class SecretMetadata(namedtuple('SecretMetadata', ['key'])):

    @staticmethod
    def create_from_jschema(j_secret_metadata):
        return SecretMetadata(key=j_secret_metadata.key())


class SecretsHandler:
    def __init__(self, secrets):
        self.secrets = secrets

    def __call__(self):
        return self

    def create_list_from_jschema(self, jschema, create_obj_from_jschema):
        return [create_obj_from_jschema(jschema.apply(i)) for i in range(jschema.length())]

    def setToken(self, token):
        self.secrets.setToken(token)

    def get(self, scope, key):
        return self.secrets.get(scope, key)

    def getBytes(self, scope, key):
        return self.secrets.getBytes(scope, key)

    def list(self, scope):
        return self.create_list_from_jschema(
            self.secrets.list(scope), SecretMetadata.create_from_jschema)

    def listScopes(self):
        return self.create_list_from_jschema(
            self.secrets.listScopes(), SecretScope.create_from_jschema)

    def __repr__(self):
        return "Package 'dbutils.secrets'. For more information, type " + \
            "'dbutils.secrets.help()' in a cell."
