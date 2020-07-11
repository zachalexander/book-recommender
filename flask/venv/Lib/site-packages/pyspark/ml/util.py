#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import concurrent.futures
import importlib
import json
import os
import six
import sys
import time
import traceback
import uuid
import warnings

import numpy as np

if sys.version > '3':
    basestring = str
    unicode = str
    long = int

try:
    _mlflow = importlib.import_module("mlflow")
    _MlflowClient = getattr(importlib.import_module('mlflow.tracking.client'), 'MlflowClient')
    _get_experiment_id = getattr(importlib.import_module('mlflow.tracking.fluent'),
                                 '_get_experiment_id')
except ImportError:
    _mlflow = None
    _MlflowClient = None
    _get_experiment_id = None

from pyspark import SparkContext, since
from pyspark.ml.common import inherit_doc
from pyspark.sql import SparkSession
from pyspark.util import VersionUtils


def _jvm():
    """
    Returns the JVM view associated with SparkContext. Must be called
    after SparkContext is initialized.
    """
    jvm = SparkContext._jvm
    if jvm:
        return jvm
    else:
        raise AttributeError("Cannot load _jvm from SparkContext. Is SparkContext initialized?")


class Identifiable(object):
    """
    Object with a unique ID.
    """

    def __init__(self):
        #: A unique id for the object.
        self.uid = self._randomUID()

    def __repr__(self):
        return self.uid

    @classmethod
    def _randomUID(cls):
        """
        Generate a unique unicode id for the object. The default implementation
        concatenates the class name, "_", and 12 random hex chars.
        """
        return unicode(cls.__name__ + "_" + uuid.uuid4().hex[-12:])


@inherit_doc
class BaseReadWrite(object):
    """
    Base class for MLWriter and MLReader. Stores information about the SparkContext
    and SparkSession.

    .. versionadded:: 2.3.0
    """

    def __init__(self):
        self._sparkSession = None

    def context(self, sqlContext):
        """
        Sets the Spark SQLContext to use for saving/loading.

        .. note:: Deprecated in 2.1 and will be removed in 3.0, use session instead.
        """
        raise NotImplementedError("Read/Write is not yet implemented for type: %s" % type(self))

    def session(self, sparkSession):
        """
        Sets the Spark Session to use for saving/loading.
        """
        self._sparkSession = sparkSession
        return self

    @property
    def sparkSession(self):
        """
        Returns the user-specified Spark Session or the default.
        """
        if self._sparkSession is None:
            self._sparkSession = SparkSession.builder.getOrCreate()
        return self._sparkSession

    @property
    def sc(self):
        """
        Returns the underlying `SparkContext`.
        """
        return self.sparkSession.sparkContext


@inherit_doc
class MLWriter(BaseReadWrite):
    """
    Utility class that can save ML instances.

    .. versionadded:: 2.0.0
    """

    def __init__(self):
        super(MLWriter, self).__init__()
        self.shouldOverwrite = False

    def _handleOverwrite(self, path):
        from pyspark.ml.wrapper import JavaWrapper

        _java_obj = JavaWrapper._new_java_obj("org.apache.spark.ml.util.FileSystemOverwrite")
        wrapper = JavaWrapper(_java_obj)
        wrapper._call_java("handleOverwrite", path, True, self.sc._jsc.sc())

    def save(self, path):
        """Save the ML instance to the input path."""
        if self.shouldOverwrite:
            self._handleOverwrite(path)
        self.saveImpl(path)

    def saveImpl(self, path):
        """
        save() handles overwriting and then calls this method.  Subclasses should override this
        method to implement the actual saving of the instance.
        """
        raise NotImplementedError("MLWriter is not yet implemented for type: %s" % type(self))

    def overwrite(self):
        """Overwrites if the output path already exists."""
        self.shouldOverwrite = True
        return self


@inherit_doc
class GeneralMLWriter(MLWriter):
    """
    Utility class that can save ML instances in different formats.

    .. versionadded:: 2.4.0
    """

    def format(self, source):
        """
        Specifies the format of ML export (e.g. "pmml", "internal", or the fully qualified class
        name for export).
        """
        self.source = source
        return self


@inherit_doc
class JavaMLWriter(MLWriter):
    """
    (Private) Specialization of :py:class:`MLWriter` for :py:class:`JavaParams` types
    """

    def __init__(self, instance):
        super(JavaMLWriter, self).__init__()
        _java_obj = instance._to_java()
        self._jwrite = _java_obj.write()

    def save(self, path):
        """Save the ML instance to the input path."""
        if not isinstance(path, basestring):
            raise TypeError("path should be a basestring, got type %s" % type(path))
        self._jwrite.save(path)

    def overwrite(self):
        """Overwrites if the output path already exists."""
        self._jwrite.overwrite()
        return self

    def option(self, key, value):
        self._jwrite.option(key, value)
        return self

    def context(self, sqlContext):
        """
        Sets the SQL context to use for saving.

        .. note:: Deprecated in 2.1 and will be removed in 3.0, use session instead.
        """
        warnings.warn(
            "Deprecated in 2.1 and will be removed in 3.0, use session instead.",
            DeprecationWarning)
        self._jwrite.context(sqlContext._ssql_ctx)
        return self

    def session(self, sparkSession):
        """Sets the Spark Session to use for saving."""
        self._jwrite.session(sparkSession._jsparkSession)
        return self


@inherit_doc
class GeneralJavaMLWriter(JavaMLWriter):
    """
    (Private) Specialization of :py:class:`GeneralMLWriter` for :py:class:`JavaParams` types
    """

    def __init__(self, instance):
        super(GeneralJavaMLWriter, self).__init__(instance)

    def format(self, source):
        """
        Specifies the format of ML export (e.g. "pmml", "internal", or the fully qualified class
        name for export).
        """
        self._jwrite.format(source)
        return self


@inherit_doc
class MLWritable(object):
    """
    Mixin for ML instances that provide :py:class:`MLWriter`.

    .. versionadded:: 2.0.0
    """

    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        raise NotImplementedError("MLWritable is not yet implemented for type: %r" % type(self))

    def save(self, path):
        """Save this ML instance to the given path, a shortcut of 'write().save(path)'."""
        self.write().save(path)


@inherit_doc
class JavaMLWritable(MLWritable):
    """
    (Private) Mixin for ML instances that provide :py:class:`JavaMLWriter`.
    """

    def write(self):
        """Returns an MLWriter instance for this ML instance."""
        return JavaMLWriter(self)


@inherit_doc
class GeneralJavaMLWritable(JavaMLWritable):
    """
    (Private) Mixin for ML instances that provide :py:class:`GeneralJavaMLWriter`.
    """

    def write(self):
        """Returns an GeneralMLWriter instance for this ML instance."""
        return GeneralJavaMLWriter(self)


@inherit_doc
class MLReader(BaseReadWrite):
    """
    Utility class that can load ML instances.

    .. versionadded:: 2.0.0
    """

    def __init__(self):
        super(MLReader, self).__init__()

    def load(self, path):
        """Load the ML instance from the input path."""
        raise NotImplementedError("MLReader is not yet implemented for type: %s" % type(self))


@inherit_doc
class JavaMLReader(MLReader):
    """
    (Private) Specialization of :py:class:`MLReader` for :py:class:`JavaParams` types
    """

    def __init__(self, clazz):
        super(JavaMLReader, self).__init__()
        self._clazz = clazz
        self._jread = self._load_java_obj(clazz).read()

    def load(self, path):
        """Load the ML instance from the input path."""
        if not isinstance(path, basestring):
            raise TypeError("path should be a basestring, got type %s" % type(path))
        java_obj = self._jread.load(path)
        if not hasattr(self._clazz, "_from_java"):
            raise NotImplementedError("This Java ML type cannot be loaded into Python currently: %r"
                                      % self._clazz)
        return self._clazz._from_java(java_obj)

    def context(self, sqlContext):
        """
        Sets the SQL context to use for loading.

        .. note:: Deprecated in 2.1 and will be removed in 3.0, use session instead.
        """
        warnings.warn(
            "Deprecated in 2.1 and will be removed in 3.0, use session instead.",
            DeprecationWarning)
        self._jread.context(sqlContext._ssql_ctx)
        return self

    def session(self, sparkSession):
        """Sets the Spark Session to use for loading."""
        self._jread.session(sparkSession._jsparkSession)
        return self

    @classmethod
    def _java_loader_class(cls, clazz):
        """
        Returns the full class name of the Java ML instance. The default
        implementation replaces "pyspark" by "org.apache.spark" in
        the Python full class name.
        """
        java_package = clazz.__module__.replace("pyspark", "org.apache.spark")
        if clazz.__name__ in ("Pipeline", "PipelineModel"):
            # Remove the last package name "pipeline" for Pipeline and PipelineModel.
            java_package = ".".join(java_package.split(".")[0:-1])
        return java_package + "." + clazz.__name__

    @classmethod
    def _load_java_obj(cls, clazz):
        """Load the peer Java object of the ML instance."""
        java_class = cls._java_loader_class(clazz)
        java_obj = _jvm()
        for name in java_class.split("."):
            java_obj = getattr(java_obj, name)
        return java_obj


@inherit_doc
class MLReadable(object):
    """
    Mixin for instances that provide :py:class:`MLReader`.

    .. versionadded:: 2.0.0
    """

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        raise NotImplementedError("MLReadable.read() not implemented for type: %r" % cls)

    @classmethod
    def load(cls, path):
        """Reads an ML instance from the input path, a shortcut of `read().load(path)`."""
        return cls.read().load(path)


@inherit_doc
class JavaMLReadable(MLReadable):
    """
    (Private) Mixin for instances that provide JavaMLReader.
    """

    @classmethod
    def read(cls):
        """Returns an MLReader instance for this class."""
        return JavaMLReader(cls)


@inherit_doc
class JavaPredictionModel():
    """
    (Private) Java Model for prediction tasks (regression and classification).
    To be mixed in with class:`pyspark.ml.JavaModel`
    """

    @property
    @since("2.1.0")
    def numFeatures(self):
        """
        Returns the number of features the model was trained on. If unknown, returns -1
        """
        return self._call_java("numFeatures")


@inherit_doc
class DefaultParamsWritable(MLWritable):
    """
    .. note:: DeveloperApi

    Helper trait for making simple :py:class:`Params` types writable.  If a :py:class:`Params`
    class stores all data as :py:class:`Param` values, then extending this trait will provide
    a default implementation of writing saved instances of the class.
    This only handles simple :py:class:`Param` types; e.g., it will not handle
    :py:class:`Dataset`. See :py:class:`DefaultParamsReadable`, the counterpart to this trait.

    .. versionadded:: 2.3.0
    """

    def write(self):
        """Returns a DefaultParamsWriter instance for this class."""
        from pyspark.ml.param import Params

        if isinstance(self, Params):
            return DefaultParamsWriter(self)
        else:
            raise TypeError("Cannot use DefautParamsWritable with type %s because it does not " +
                            " extend Params.", type(self))


@inherit_doc
class DefaultParamsWriter(MLWriter):
    """
    .. note:: DeveloperApi

    Specialization of :py:class:`MLWriter` for :py:class:`Params` types

    Class for writing Estimators and Transformers whose parameters are JSON-serializable.

    .. versionadded:: 2.3.0
    """

    def __init__(self, instance):
        super(DefaultParamsWriter, self).__init__()
        self.instance = instance

    def saveImpl(self, path):
        DefaultParamsWriter.saveMetadata(self.instance, path, self.sc)

    @staticmethod
    def saveMetadata(instance, path, sc, extraMetadata=None, paramMap=None):
        """
        Saves metadata + Params to: path + "/metadata"
        - class
        - timestamp
        - sparkVersion
        - uid
        - paramMap
        - defaultParamMap (since 2.4.0)
        - (optionally, extra metadata)
        :param extraMetadata:  Extra metadata to be saved at same level as uid, paramMap, etc.
        :param paramMap:  If given, this is saved in the "paramMap" field.
        """
        metadataPath = os.path.join(path, "metadata")
        metadataJson = DefaultParamsWriter._get_metadata_to_save(instance,
                                                                 sc,
                                                                 extraMetadata,
                                                                 paramMap)
        sc.parallelize([metadataJson], 1).saveAsTextFile(metadataPath)

    @staticmethod
    def _get_metadata_to_save(instance, sc, extraMetadata=None, paramMap=None):
        """
        Helper for :py:meth:`DefaultParamsWriter.saveMetadata` which extracts the JSON to save.
        This is useful for ensemble models which need to save metadata for many sub-models.

        .. note:: :py:meth:`DefaultParamsWriter.saveMetadata` for details on what this includes.
        """
        uid = instance.uid
        cls = instance.__module__ + '.' + instance.__class__.__name__

        # User-supplied param values
        params = instance._paramMap
        jsonParams = {}
        if paramMap is not None:
            jsonParams = paramMap
        else:
            for p in params:
                jsonParams[p.name] = params[p]

        # Default param values
        jsonDefaultParams = {}
        for p in instance._defaultParamMap:
            jsonDefaultParams[p.name] = instance._defaultParamMap[p]

        basicMetadata = {"class": cls, "timestamp": long(round(time.time() * 1000)),
                         "sparkVersion": sc.version, "uid": uid, "paramMap": jsonParams,
                         "defaultParamMap": jsonDefaultParams}
        if extraMetadata is not None:
            basicMetadata.update(extraMetadata)
        return json.dumps(basicMetadata, separators=[',',  ':'])


@inherit_doc
class DefaultParamsReadable(MLReadable):
    """
    .. note:: DeveloperApi

    Helper trait for making simple :py:class:`Params` types readable.
    If a :py:class:`Params` class stores all data as :py:class:`Param` values,
    then extending this trait will provide a default implementation of reading saved
    instances of the class. This only handles simple :py:class:`Param` types;
    e.g., it will not handle :py:class:`Dataset`. See :py:class:`DefaultParamsWritable`,
    the counterpart to this trait.

    .. versionadded:: 2.3.0
    """

    @classmethod
    def read(cls):
        """Returns a DefaultParamsReader instance for this class."""
        return DefaultParamsReader(cls)


@inherit_doc
class DefaultParamsReader(MLReader):
    """
    .. note:: DeveloperApi

    Specialization of :py:class:`MLReader` for :py:class:`Params` types

    Default :py:class:`MLReader` implementation for transformers and estimators that
    contain basic (json-serializable) params and no data. This will not handle
    more complex params or types with data (e.g., models with coefficients).

    .. versionadded:: 2.3.0
    """

    def __init__(self, cls):
        super(DefaultParamsReader, self).__init__()
        self.cls = cls

    @staticmethod
    def __get_class(clazz):
        """
        Loads Python class from its name.
        """
        parts = clazz.split('.')
        module = ".".join(parts[:-1])
        m = __import__(module)
        for comp in parts[1:]:
            m = getattr(m, comp)
        return m

    def load(self, path):
        metadata = DefaultParamsReader.loadMetadata(path, self.sc)
        py_type = DefaultParamsReader.__get_class(metadata['class'])
        instance = py_type()
        instance._resetUid(metadata['uid'])
        DefaultParamsReader.getAndSetParams(instance, metadata)
        return instance

    @staticmethod
    def loadMetadata(path, sc, expectedClassName=""):
        """
        Load metadata saved using :py:meth:`DefaultParamsWriter.saveMetadata`

        :param expectedClassName:  If non empty, this is checked against the loaded metadata.
        """
        metadataPath = os.path.join(path, "metadata")
        metadataStr = sc.textFile(metadataPath, 1).first()
        loadedVals = DefaultParamsReader._parseMetaData(metadataStr, expectedClassName)
        return loadedVals

    @staticmethod
    def _parseMetaData(metadataStr, expectedClassName=""):
        """
        Parse metadata JSON string produced by :py:meth`DefaultParamsWriter._get_metadata_to_save`.
        This is a helper function for :py:meth:`DefaultParamsReader.loadMetadata`.

        :param metadataStr:  JSON string of metadata
        :param expectedClassName:  If non empty, this is checked against the loaded metadata.
        """
        metadata = json.loads(metadataStr)
        className = metadata['class']
        if len(expectedClassName) > 0:
            assert className == expectedClassName, "Error loading metadata: Expected " + \
                "class name {} but found class name {}".format(expectedClassName, className)
        return metadata

    @staticmethod
    def getAndSetParams(instance, metadata):
        """
        Extract Params from metadata, and set them in the instance.
        """
        # Set user-supplied param values
        for paramName in metadata['paramMap']:
            param = instance.getParam(paramName)
            paramValue = metadata['paramMap'][paramName]
            instance.set(param, paramValue)

        # Set default param values
        majorAndMinorVersions = VersionUtils.majorMinorVersion(metadata['sparkVersion'])
        major = majorAndMinorVersions[0]
        minor = majorAndMinorVersions[1]

        # For metadata file prior to Spark 2.4, there is no default section.
        if major > 2 or (major == 2 and minor >= 4):
            assert 'defaultParamMap' in metadata, "Error loading metadata: Expected " + \
                "`defaultParamMap` section not found"

            for paramName in metadata['defaultParamMap']:
                paramValue = metadata['defaultParamMap'][paramName]
                instance._setDefault(**{paramName: paramValue})

    @staticmethod
    def loadParamsInstance(path, sc):
        """
        Load a :py:class:`Params` instance from the given path, and return it.
        This assumes the instance inherits from :py:class:`MLReadable`.
        """
        metadata = DefaultParamsReader.loadMetadata(path, sc)
        pythonClassName = metadata['class'].replace("org.apache.spark", "pyspark")
        py_type = DefaultParamsReader.__get_class(pythonClassName)
        instance = py_type.load(path)
        return instance


class _AsynchronousTaskQueue:
    """
    Helper class to start a async call with ThreadPoolExecutor
    """

    def __init__(self):
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.futures = []

    def __del__(self):
        self.thread_pool.shutdown()

    def call(self, async_task_func, **kwargs):
        future = self.thread_pool.submit(async_task_func, **kwargs)
        self.futures.append(future)


class _MLflowUsageLogger:
    """
    Helper class for usage logging.
    """
    def __init__(self):
        try:
            self.logger = SparkContext._jvm.com.databricks.spark.util.PythonUsageLoggingImpl()
        except Exception:
            self.logger = None

    def log_params(self, params):
        if self.logger is None:
            return
        self.logger.recordEvent(
            self.logger.metricDefinitions().EVENT_ML_UTIL(),
            {
                self.logger.tagDefinitions().TAG_ML_UTIL_TYPE(): 'mllibAutoTracking'
            },
            json.dumps(params, separators=[',',  ':'])
        )

    def log_failure(self, err, message):
        if self.logger is None:
            return
        self.logger.recordEvent(
            self.logger.metricDefinitions().EVENT_ML_UTIL(),
            {
                self.logger.tagDefinitions().TAG_ML_UTIL_TYPE(): 'mllibAutoTracking',
                self.logger.tagDefinitions().TAG_STATUS(): "failure",
                self.logger.tagDefinitions().TAG_ERROR(): err,
                self.logger.tagDefinitions().TAG_ERROR_MESSAGE(): message,
            },
            ""
        )


class _MLflowCompat(object):
    """
    This class provides backward compatibility for MLflow
    """

    @staticmethod
    def to_dict(x):
        """
        MLflow master changes some objects from list to dict. This function allows
        accessing both types uniformly.
        """
        if isinstance(x, dict):
            return x
        # All these objects have a key and a value
        return dict([(i.key, i.value) for i in x])


class _MLflowInstrumentation:
    """
    Helper functions for MLflow logging
    """

    async_task_queue = _AsynchronousTaskQueue()
    mlflow_client = None
    usage_logger = None

    _MLFLOW_TRACKING_ENABLED_FLAG = "spark.databricks.mlflow.trackMLlib.enabled"

    _MLFLOW_TRACKING_DISABLED_MESSAGE =\
        "MLflow auto-tracking of MLlib tuning is currently disabled.  To enable this feature, " \
        "set the flag via `spark.conf.set('{flag}', 'true')` where `spark` is your SparkSession."\
        .format(flag=_MLFLOW_TRACKING_ENABLED_FLAG)

    _NO_MLFLOW_WARNING =\
        "Can not find mlflow. To enable mlflow logging, install MLflow library from PyPi."

    _MLFLOW_UNREACHABLE_WARNING = \
        "MLlib could not reach the MLflow server at tracking URI: {uri}"

    _INVALID_EXPERIMENT_ID_WARNING = \
        "The current or default MLflow experiment ID is {id}, which is invalid.  " \
        "This can happen in Databricks Jobs currently, but it can be fixed by explicitly " \
        "setting an MLflow experiment in your Job."

    _HAVE_MLFLOW_MESSAGE = \
        "MLlib will automatically track trials in MLflow. After your tuning fit() call has " \
        "completed, view the MLflow UI to see logged runs."

    @staticmethod
    def _get_fit_uuid():
        """
        Generates a UUID for a single fit() call.
        """
        return str(uuid.uuid4())[:6]

    @staticmethod
    def _append_fit_uuid(fit_uuid, d):
        """
        Appends a fit() UUID to the keys of a dictionary.
        :param d: dict to be updated
        :return: the updated dict
        """
        return dict([(k + "_" + fit_uuid, v) for k, v in d.items()])

    @staticmethod
    def _func_call_catch_exception(func, **kwargs):
        try:
            func(**kwargs)
        except Exception as exc:
            exc_str = six.StringIO()
            try:
                traceback.print_exc(file=exc_str)
                warnings.warn("Logging to MLflow failed. Tuning will run without MLflow logging. "
                              "Caught {exc_type} with message: {exc_message}\n"
                              "{stack}".format(exc_type=type(exc).__name__, exc_message=str(exc),
                                               stack=exc_str.getvalue()))
            finally:
                exc_str.close()
            _MLflowInstrumentation.usage_logger.log_failure(type(exc).__name__, str(exc))

    @staticmethod
    def is_tracking_enabled(spark):
        """
        :param spark: SparkSession
        :return: True if MLlib auto-tracking is feature flagged on.  False otherwise.
        """
        flag = spark.conf.get(_MLflowInstrumentation._MLFLOW_TRACKING_ENABLED_FLAG, default='true')
        return flag.lower() == 'true'

    @staticmethod
    def get_mlflow_logging_status(spark):
        """
        This helper method should be called at the beginning of tuning fit() calls
        to notify the user of whether MLflow auto-tracking will happen.
        This reports these cases:
         - MLlib auto-tracking not enabled by feature flag
         - MLflow library not available
         - MLflow server cannot be reached
         - MLflow tracking will happen
        :param spark: SparkSession
        :return: True if tuning should log to MLflow; False otherwise.
        """
        if not _MLflowInstrumentation.is_tracking_enabled(spark):
            warnings.warn(_MLflowInstrumentation._MLFLOW_TRACKING_DISABLED_MESSAGE)
            return False
        if _mlflow is None:
            warnings.warn(_MLflowInstrumentation._NO_MLFLOW_WARNING)
            return False
        tracking_uri = _mlflow.tracking.get_tracking_uri()
        try:
            # canary query to server (which returns None if the experiment does not exist)
            mlflow_client = _MlflowClient()
            mlflow_client.get_experiment_by_name("blah")
        except Exception as e:
            warnings.warn(
                _MLflowInstrumentation._MLFLOW_UNREACHABLE_WARNING.format(uri=tracking_uri) +
                "\nException: {e}".format(e=e))
            return False
        exp_id = _get_experiment_id()
        # MLflow docs say the experiment ID is an int, but it is returned as a str,
        # so we check both here for safety.
        if exp_id == 0 or exp_id == '0':
            warnings.warn(_MLflowInstrumentation._INVALID_EXPERIMENT_ID_WARNING.format(id=exp_id))
            return False
        print(_MLflowInstrumentation._HAVE_MLFLOW_MESSAGE)
        return True

    @staticmethod
    def log_crossvalidator(cv, metrics):
        """
        Log CrossValidator fit.
        WARNING: This method is NOT thread-safe.  It assumes that the active MLflow run (if any)
        does not change during the call to this method.

        :param cv: CrossValidator
        :param metrics: numModels by nFolds metric array. Each row is the metrics for one fold
        """
        _MLflowInstrumentation._log_generic_tuning(_MLflowInstrumentation._log_cv, cv, metrics)

    @staticmethod
    def _log_generic_tuning(logging_func, tuning, metrics):
        """
        Log CrossValidator or TrainValidationSplit fit.
        WARNING: This method is NOT thread-safe.  It assumes that the active MLflow run (if any)
        does not change during the call to this method.

        :param logging_func: _log_cv or _log_tvs
        :param tuning: CrossValidator or TrainValidationSplit instance
        :param metrics: numModels by nFolds metric array. Each row is the metrics for one fold
        """

        if _mlflow is None:
            return
        _MLflowInstrumentation.mlflow_client = _MlflowClient()
        if _MLflowInstrumentation.usage_logger is None:
            _MLflowInstrumentation.usage_logger = _MLflowUsageLogger()
        fit_uuid = _MLflowInstrumentation._get_fit_uuid()

        should_append_fit_uuid = False
        active_run = _mlflow.active_run()
        should_end_run = active_run is None
        if active_run is not None:
            run_uuid = active_run.info.run_uuid
            tags = _MLflowInstrumentation.mlflow_client.get_run(run_uuid).data.tags
            should_append_fit_uuid = 'fit_uuid' in _MLflowCompat.to_dict(tags)
        else:
            run_uuid = _mlflow.start_run(nested=True).info.run_uuid
        _MLflowInstrumentation.async_task_queue.call(
            async_task_func=_MLflowInstrumentation._func_call_catch_exception,
            func=logging_func,
            run_uuid=run_uuid,
            fit_uuid=fit_uuid,
            should_append_fit_uuid=should_append_fit_uuid,
            tuning=tuning,
            metrics=metrics)
        if should_end_run:
            _mlflow.end_run()

    @staticmethod
    def _log_cv(run_uuid, fit_uuid, should_append_fit_uuid, tuning, metrics):
        est = tuning.getOrDefault(tuning.estimator)
        epm = tuning.getOrDefault(tuning.estimatorParamMaps)
        nFolds = tuning.getOrDefault(tuning.numFolds)
        eva = tuning.getOrDefault(tuning.evaluator)

        params = _MLflowInstrumentation._get_common_info(tuning, epm, eva)
        params['numFolds'] = nFolds
        _MLflowInstrumentation._log_tuning_info(run_uuid, fit_uuid, params, should_append_fit_uuid)

        metric_names = ['avg_'+eva.getMetricName(), 'std_'+eva.getMetricName()]
        metrics = np.stack((np.mean(metrics, axis=0), np.std(metrics, axis=0)))
        _MLflowInstrumentation._log_tuning_runs(run_uuid, fit_uuid, est, epm, metric_names, metrics)

    @staticmethod
    def log_trainvalidationsplit(tvs, metrics):
        """
        Log TrainValidationSplit fit.
        WARNING: This method is NOT thread-safe.  It assumes that the active MLflow run (if any)
        does not change during the call to this method.

        :param tvs: TrainValidationSplit
        :param metrics: metrics array, each element is a single metric.
        """
        _MLflowInstrumentation._log_generic_tuning(_MLflowInstrumentation._log_tvs, tvs, metrics)

    @staticmethod
    def _log_tvs(run_uuid, fit_uuid, should_append_fit_uuid, tuning, metrics):
        _MLflowInstrumentation.mlflow_client = _MlflowClient()

        est = tuning.getOrDefault(tuning.estimator)
        epm = tuning.getOrDefault(tuning.estimatorParamMaps)
        eva = tuning.getOrDefault(tuning.evaluator)

        params = _MLflowInstrumentation._get_common_info(tuning, epm, eva)
        _MLflowInstrumentation._log_tuning_info(run_uuid, fit_uuid, params, should_append_fit_uuid)

        metric_names = [eva.getMetricName()]
        _MLflowInstrumentation._log_tuning_runs(run_uuid, fit_uuid, est, epm,
                                                metric_names, np.array([metrics]))

    @staticmethod
    def _get_common_info(tuning_alg, epm, eva):
        """
        Returns a dict of information common to both CrossValidator and TrainValidationSplit
        """

        est = tuning_alg.getOrDefault(tuning_alg.estimator)
        params = {'mlModelClass': tuning_alg.__class__.__name__,
                  'mlEstimatorUid': tuning_alg.uid,
                  'estimator': type(est).__name__,
                  'evaluator': type(eva).__name__,
                  'estimatorParamMapsLength': len(epm)}
        return params

    @staticmethod
    def _log_tuning_info(run_uuid, fit_uuid, params, should_append_fit_uuid):
        """
        Log information common to both CrossValidator and TrainValidationSplit
        This logs to both MLflow and Databricks usage logging.
        :param params: Dict of params from :py:func:`_get_common_info`, possibly augmented
        :param should_append_fit_uuid: If true, append fit_uuid to each param and to the
                                       'fit_uuid' tag for this run.
        """
        if _mlflow is None:
            return
        mlflow_client = _MLflowInstrumentation.mlflow_client
        if should_append_fit_uuid:
            params = _MLflowInstrumentation._append_fit_uuid(fit_uuid, params)
            mlflow_client.set_tag(run_uuid, 'fit_uuid' + '_' + fit_uuid, fit_uuid)
        else:
            mlflow_client.set_tag(run_uuid, 'fit_uuid', fit_uuid)

        _MLflowInstrumentation.usage_logger.log_params(params)
        for param_name in params:
            mlflow_client.log_param(run_uuid, param_name, params[param_name])
        mlflow_client.set_tag(run_uuid, 'runSource', "mllibAutoTracking")

    @staticmethod
    def _log_tuning_runs(parent_run_uuid, fit_uuid, est, epm, metric_names, metrics):
        """
        Log multiple runs from paramMap and metrics
        """
        if _mlflow is None:
            return

        num_models = len(epm)
        assert num_models == metrics.shape[1]

        if num_models == 0:
            return
        param_to_name = _MLflowInstrumentation._resolve_param_names(epm[0].keys())
        tags = {'mlflow.parentRunId': parent_run_uuid, 'fit_uuid': fit_uuid}
        for index in range(num_models):
            exp_id_for_run = _get_experiment_id()
            active_run = _MLflowInstrumentation.mlflow_client.create_run(
                experiment_id=exp_id_for_run, tags=tags)
            run_uuid = active_run.info.run_uuid
            _MLflowInstrumentation._log_single_run(run_uuid,
                                                   est,
                                                   epm[index],
                                                   param_to_name,
                                                   metric_names,
                                                   metrics[:, index])

    @staticmethod
    def _log_single_run(run_uuid, est, param_map, param_to_name, metric_names, metrics):
        """
        Log single run from paramMap and metrics
        :param run_uuid: mlflow run uuid
        :param est: Estimator for this run
        :param param_map: ParamMap for this run
        :param param_to_name: dict mapping each Param to the str name to be used for MLflow logging
        :param metric_names: list of metric names to be logged
        :param metrics: list of metric values to be logged, in the same order as metric_names
        """
        if _mlflow is None:
            return
        mlflow_client = _MLflowInstrumentation.mlflow_client

        assert len(metric_names) == len(metrics)
        mlflow_client.log_param(run_uuid, 'mlModelClass', type(est).__name__)
        mlflow_client.log_param(run_uuid, 'mlEstimatorUid', est.uid)
        mlflow_client.set_tag(run_uuid, 'runSource', "mllibAutoTracking")
        for key in param_map:
            mlflow_client.log_param(run_uuid, param_to_name[key], param_map[key])
        for i in range(len(metric_names)):
            mlflow_client.log_metric(run_uuid, metric_names[i], metrics[i])

    @staticmethod
    def _resolve_param_names(params):
        """
        Handle conflict resolution for ParamMaps with multiple Params of the same name.
        This renames Params as follows:
         - Use Param.name if there are no conflicts: `maxIter`
         - Add prefix Param.parent.uid + '.' if needed: `KMeans_529d38e73a74.maxIter`
        :param params: list of Params which will be logged within a single run
        :return: dict mapping Params to names usable for logging
        """
        from collections import defaultdict

        # Group Params by name
        grouped = defaultdict(list)
        for param in params:
            grouped[param.name].append(param)

        # Resolve names
        resolved = dict()  # Param -> final name
        for param_name, duplicate_params in grouped.items():
            if len(duplicate_params) == 1:
                resolved[duplicate_params[0]] = param_name
            else:
                for p in duplicate_params:
                    resolved[p] = p.parent + '.' + p.name

        return resolved
