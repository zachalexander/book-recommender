#!/usr/bin/env python
# DATABRICKS CONFIDENTIAL & PROPRIETARY
# __________________
#
# Copyright 2019 Databricks, Inc.
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of Databricks, Inc.
# and its suppliers, if any.  The intellectual and technical concepts contained herein are
# proprietary to Databricks, Inc. and its suppliers and may be covered by U.S. and foreign Patents,
# patents in process, and are protected by trade secret and/or copyright law. Dissemination, use,
# or reproduction of this information is strictly forbidden unless prior written permission is
# obtained from Databricks, Inc.
#
# If you view or obtain a copy of this information and believe Databricks, Inc. may not have
# intended it to be made available, please promptly report it to Databricks Legal Department
# @ legal@databricks.com.

from __future__ import print_function

import argparse
import json
import os
import subprocess

try:
    import __builtin__
    input = getattr(__builtin__, 'raw_input')
except (ImportError, AttributeError):
    pass

parser = argparse.ArgumentParser()
parser.add_argument("command", choices=["configure", "test", "get-jar-dir", "get-spark-home"])

CONFIG_PATH = os.path.expanduser("~/.databricks-connect")

CLUSTER_HELP = """IMPORTANT: please ensure that your cluster has:
- Databricks Runtime version of DBR 5.1+
- Python version same as your local Python (i.e., 2.7 or 3.5)
- the Spark conf `spark.databricks.service.server.enabled true` set
"""

EULA = """Copyright (2018) Databricks, Inc.

This library (the "Software") may not be used except in connection with the
Licensee's use of the Databricks Platform Services pursuant to an Agreement
(defined below) between Licensee (defined below) and Databricks, Inc.
("Databricks"). This Software shall be deemed part of the "Subscription
Services" under the Agreement, or if the Agreement does not define
Subscription Services, then the term in such Agreement that refers to the
applicable Databricks Platform Services (as defined below) shall be
substituted herein for "Subscription Services."  Licensee's use of the
Software must comply at all times with any restrictions applicable to the
Subscription Services, generally, and must be used in accordance with any
applicable documentation. If you have not agreed to an Agreement or otherwise
do not agree to these terms, you may not use the Software.  This license
terminates automatically upon the termination of the Agreement or Licensee's
breach of these terms.

Agreement: the agreement between Databricks and Licensee governing the use of
the Databricks Platform Services, which shall be, with respect to Databricks,
the Databricks Terms of Service located at www.databricks.com/termsofservice,
and with respect to Databricks Community Edition, the Community Edition Terms
of Service located at www.databricks.com/ce-termsofuse, in each case unless
Licensee has entered into a separate written agreement with Databricks
governing the use of the applicable Databricks Platform Services.

Databricks Platform Services: the Databricks services or the Databricks
Community Edition services, according to where the Software is used.

Licensee: the user of the Software, or, if the Software is being used on
behalf of a company, the company.
"""


def print_config(host, token, cluster, org_id, port):
    print("* Databricks Host: {}".format(host))
    print("* Databricks Token: {}".format(token))
    print("* Cluster ID: {}".format(cluster))
    print("* Org ID: {}".format(org_id))
    print("* Port: {}".format(port))


def save_config(host, token, cluster, org_id, port):
    # note: all values are string type for easy JSON parsing as Map[String, String] in scala
    def serialize(value):
        if value is None:
            return None
        else:
            return str(value)

    with open(CONFIG_PATH, "w") as f:
        f.write(json.dumps({
            "host": serialize(host),
            "token": serialize(token),
            "cluster_id": serialize(cluster),
            "org_id": serialize(org_id),
            "port": serialize(port),
        }, indent=2))


def configure():
    host, token, cluster, org_id, port = None, None, None, None, None
    if os.path.exists(CONFIG_PATH):
        try:
            config = json.loads(open(CONFIG_PATH).read())
            host, token, cluster, org_id, port = \
                config.get("host"), config.get("token"), config.get("cluster_id"), \
                config.get("org_id", 0), config.get("port", 15001)
            print("The current configuration is:")
            print_config(host, token, cluster, org_id, port)
        except Exception as e:
            print("Failed to parse existing config in {}:".format(CONFIG_PATH), e)
    else:
        print(EULA)
        print("Do you accept the above agreement? [y/N] ", end="")
        accept = input().strip()
        if accept.lower() != "y":
            print("You must accept the license agreement to continue.")
            exit(0)

    print("Set new config values (leave input empty to accept default):")
    print("Databricks Host [{}]: ".format(
        host or "no current value, must start with https://"), end="")
    new_host = input().strip()
    if new_host:
        host = new_host
    if not host or not host.startswith("https://"):
        raise ValueError(
            "New host value must start with https://, e.g., https://demo.cloud.databricks.com")

    save_config(host, token, cluster, org_id, port)

    print("Databricks Token [{}]: ".format(token or "no current value"), end="")
    new_token = input().strip()
    if new_token:
        token = new_token
    if not token:
        raise ValueError("Token value must be non-empty")

    save_config(host, token, cluster, org_id, port)

    print("\n{}\nCluster ID (e.g., 0921-001415-jelly628) [{}]: ".format(
        CLUSTER_HELP, cluster or "no current value"), end="")
    new_cluster = input().strip()
    if new_cluster:
        cluster = new_cluster
    if not cluster:
        raise ValueError("Cluster id value must be non-empty")

    save_config(host, token, cluster, org_id, port)

    print("Org ID (Azure-only, see ?o=orgId in URL) [{}]: ".format(org_id or "0"), end="")
    new_org_id = input().strip()
    if new_org_id:
        org_id = new_org_id
    elif org_id is None:
        org_id = 0
    int(org_id)  # check it's an int

    save_config(host, token, cluster, org_id, port)

    print("Port [{}]: ".format(port or "15001"), end="")
    new_port = input().strip()
    if new_port:
        port = new_port
    elif port is None:
        port = 15001
    int(port)  # check it's an int

    save_config(host, token, cluster, org_id, port)

    print("\nUpdated configuration in {}".format(CONFIG_PATH))
    print("* Spark jar dir: {}".format(get_jar_dir()))
    print("* Spark home: {}".format(get_spark_home()))
    print("* Run `pip install -U databricks-connect` to install updates")
    print("* Run `pyspark` to launch a Python shell")
    print("* Run `spark-shell` to launch a Scala shell")
    print("* Run `databricks-connect test` to test connectivity")
    print()
    print("Databricks Connect User Survey: https://forms.gle/V2indnHHfrjGWyQ4A")


def test():
    import pyspark
    spark_home = os.path.dirname(pyspark.__file__)
    print("* PySpark is installed at", spark_home)
    if not os.path.exists(os.path.join(spark_home, "databricks_connect.py")):
        raise EnvironmentError(
            "The `pyspark` import does not resolve to a Databricks Connect installation. "
            "Please uninstall the conflicting installation at {}. ".format(spark_home) +
            "You will need to re-install databricks-connect after doing this.")

    print("* Checking SPARK_HOME")
    if "SPARK_HOME" in os.environ:
        env_home = os.environ["SPARK_HOME"]
        if os.path.abspath(env_home) != os.path.abspath(spark_home):
            raise EnvironmentError(
                "The SPARK_HOME environment variable is set to `{}`, which conflicts with ".format(
                    env_home) +
                "the Databricks Connect installation at `{}`. ".format(spark_home) +
                "Please unset it before continuing.")

    print("* Checking java version")
    try:
        info = subprocess.check_output(
            "java -version", stderr=subprocess.STDOUT, shell=True).decode("utf-8")
        print(info.strip())
        if "1.8" not in info:
            print("WARNING: Java versions >8 are not supported by this SDK")
    except Exception as e:
        print("Failed to check java version", e)

    # Try before python since it has better error output
    if os.name == "nt":
        # it doesn't work since spark-shell doesn't take stdin on Windows
        print("* Skipping scala command test on Windows")
    else:
        print("* Testing scala command")
        try:
            output = subprocess.check_output(
                "echo 'spark.range(100).reduce(_ + _)' | spark-shell", shell=True).decode("utf-8")
        except subprocess.CalledProcessError as e:
            output = e.output.decode("utf-8")
        print(output)
        if "4950" not in output:
            raise ValueError("Scala command failed to produce correct result")

    print("* Testing python command")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    result = spark.range(100).count()
    if result != 100:
        raise ValueError("Python command failed to produce correct result", result)

    print("* Testing dbutils.fs")
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils()
    results = dbutils.fs.ls("dbfs:/")
    print(results)
    if len(results) == 0:
        raise ValueError("dbutils.fs.ls failed to produce valid result", result)

    print("* All tests passed.")


def get_jar_dir():
    return get_spark_home() + "/jars"


def get_spark_home():
    import pyspark
    spark_home = os.path.dirname(pyspark.__file__)
    return spark_home


def main():
    args = parser.parse_args()
    if args.command == "configure":
        configure()
    elif args.command == "test":
        test()
    elif args.command == "get-jar-dir":
        print(get_jar_dir())
    elif args.command == "get-spark-home":
        print(get_spark_home())


if __name__ == "__main__":
    main()
