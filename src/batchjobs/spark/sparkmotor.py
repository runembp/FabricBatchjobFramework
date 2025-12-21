def create_spark_session():
    from pyspark.sql import SparkSession
    import os

    python_exe = r"D:\source\Fabric\venv\Scripts\python.exe"  # your venv Python
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    spark = (
        SparkSession.builder
        .appName("LocalCSV")
        .master("local[*]")  # run locally using all cores
        .getOrCreate()
    )

    return spark
