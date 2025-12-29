
import sys
import os
import pytest
import socketserver
import subprocess

if sys.platform == 'win32':
    socketserver.UnixStreamServer = socketserver.TCPServer
    socketserver.UnixStreamHandler = socketserver.StreamRequestHandler

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Windows-specific configurations
    if sys.platform == 'win32':
        os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
        os.environ['JAVA_HOME'] = r'C:\Program Files\Microsoft\jdk-17.0.17.10-hotspot'
        os.environ['HADOOP_HOME'] = r'C:\spark'
        os.environ['USERNAME'] = 'spark'
        
        # Use absolute path for Python executable
        python_exe = r'C:\Users\rikuma\PythonProjects\pei-assessment-project\pei-retail-sales-pipeline\.venv\Scripts\python.exe'
        os.environ['PYSPARK_PYTHON'] = python_exe
        os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe
    
    builder = (SparkSession.builder
        .master("local[1]")
        .appName("Windows-PyTest-Local")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.hadoop.mapreduce.job.user.name", "spark")
        .config("spark.driver.extraJavaOptions", "-Dcom.sun.jndi.ldap.connect.pool=false")
        .config("spark.shuffle.service.enabled", "false")
        )
    

    session = builder.getOrCreate()
    yield session
    session.stop()
 