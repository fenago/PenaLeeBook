# Chapter 3: Data Processing

## Learning Objectives

By the end of this chapter, you will be able to:
- Explain the core architecture of Apache Spark and its role in big data processing
- Configure and customize a Spark session for different workloads
- Create and manipulate DataFrames using PySpark
- Apply transformations and actions to process data efficiently
- Execute SQL queries against Spark DataFrames
- Implement data processing patterns for common ETL scenarios
- Optimize Spark operations for better performance
- Handle complex data types and nested structures

## Key Terms

- **Apache Spark**: An open-source, distributed computing system for big data processing with in-memory caching and optimized query execution
- **PySpark**: The Python API for Apache Spark, enabling Python programmers to leverage Spark's capabilities
- **DataFrame**: A distributed collection of data organized into named columns, conceptually equivalent to a table in a relational database
- **Transformation**: A lazy operation on Spark DataFrames that creates a new DataFrame without modifying the original data
- **Action**: An operation that triggers computation and returns results to the driver program or writes data to external storage
- **Spark SQL**: A Spark module for structured data processing that provides a programming interface for SQL queries
- **Catalyst Optimizer**: Spark's query optimization engine that transforms and optimizes logical plans into physical execution plans
- **ETL (Extract, Transform, Load)**: The process of extracting data from source systems, transforming it to fit operational needs, and loading it into a target data store

## Introduction

Data processing is at the heart of any big data application. Whether you're cleaning messy datasets, joining information from disparate sources, aggregating metrics for business intelligence, or preparing features for machine learning, efficient and scalable data processing is essential. This chapter explores Apache Spark's powerful capabilities for processing data at scale using both programmatic interfaces and SQL.

Unlike traditional data processing systems that struggle with large datasets, Apache Spark is designed from the ground up to handle big data efficiently. Its distributed architecture, in-memory processing, and optimized execution engine make it possible to process terabytes of data across clusters of machines. Through PySpark, we can leverage these capabilities using Python, one of the most popular languages for data analysis.

In the previous chapter, we set up Databricks Community Edition, which provides a managed Spark environment. Now, we'll dive deeper into how Spark works and how to use it for various data processing tasks. We'll start with the fundamental concepts of Spark architecture and gradually progress to more advanced data manipulation techniques.

By the end of this chapter, you'll have a solid understanding of how to process data effectively using Apache Spark. You'll be able to write efficient PySpark code, leverage Spark SQL for declarative data processing, and implement common ETL patterns. These skills will form the foundation for more advanced topics in subsequent chapters, such as stream processing and machine learning.

```mermaid
flowchart TD
    A[Raw Data] --> B[Extract]
    B --> C[Transform]
    C --> D[Load]
    D --> E[Processed Data]
    
    subgraph "Chapter Focus"
    B
    C
    D
    end
    
    style A fill:#f5f5f5,stroke:#333,stroke-width:2px
    style B fill:#d1e7dd,stroke:#333,stroke-width:2px
    style C fill:#d1e7dd,stroke:#333,stroke-width:2px
    style D fill:#d1e7dd,stroke:#333,stroke-width:2px
    style E fill:#f5f5f5,stroke:#333,stroke-width:2px
    style Chapter fill:#f8f9fa,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
```

Let's begin our journey into the world of distributed data processing with Apache Spark.
## 1. Introduction to Apache Spark

### 1.1 Spark Architecture Overview

Apache Spark is a unified analytics engine designed for large-scale data processing. To understand how Spark processes data efficiently, we must first examine its core architecture. Spark follows a master-worker architecture with a central coordinator (the driver) and distributed workers (executors) that perform parallel processing.

```mermaid
flowchart TD
    Driver[Driver Program] --> Master[Cluster Manager]
    Master --> Worker1[Worker Node 1]
    Master --> Worker2[Worker Node 2]
    Master --> Worker3[Worker Node 3]
    
    subgraph "Worker Node 1"
    Worker1 --> Executor1[Executor]
    Executor1 --> Task1A[Task]
    Executor1 --> Task1B[Task]
    Executor1 --> Task1C[Task]
    end
    
    subgraph "Worker Node 2"
    Worker2 --> Executor2[Executor]
    Executor2 --> Task2A[Task]
    Executor2 --> Task2B[Task]
    end
    
    subgraph "Worker Node 3"
    Worker3 --> Executor3[Executor]
    Executor3 --> Task3A[Task]
    Executor3 --> Task3B[Task]
    Executor3 --> Task3C[Task]
    end
    
    style Driver fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Master fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Worker1 fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Worker2 fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Worker3 fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Executor1 fill:#ffefc0,stroke:#333,stroke-width:2px
    style Executor2 fill:#ffefc0,stroke:#333,stroke-width:2px
    style Executor3 fill:#ffefc0,stroke:#333,stroke-width:2px
```

Let's examine the key components of this architecture:

1. **Driver Program**: The driver is the process where the main application runs, creating a SparkSession or SparkContext that coordinates the execution. It's responsible for:
   - Breaking down your application into tasks
   - Scheduling tasks on executors
   - Coordinating activity between executors
   - Collecting results from executors
   - Interacting with storage systems

2. **Cluster Manager**: The cluster manager allocates resources across the cluster. Spark supports several cluster managers:
   - Standalone Scheduler (Spark's built-in manager)
   - Apache Mesos
   - Hadoop YARN
   - Kubernetes
   
   In Databricks, the cluster manager is handled automatically for you.

3. **Worker Nodes**: These are machines in the cluster that run application code. Each worker node hosts one or more executors.

4. **Executors**: These are processes launched on worker nodes. Each executor:
   - Runs tasks (fundamental units of computation in Spark)
   - Maintains data in memory or disk storage
   - Returns results to the driver

5. **Tasks**: The smallest unit of work in Spark. The driver breaks down each operation into tasks that operate on a partition of data. These tasks run in parallel across the cluster.

#### Knowledge Check

> **Question**: What is the role of the driver in a Spark application?
> 
> **Answer**: The driver program controls the overall execution of the Spark application. It creates the SparkSession, breaks down operations into tasks, schedules these tasks on executors, and coordinates the overall execution across the cluster.

### 1.2 Spark Components and Ecosystem

Spark provides a unified platform for various data processing needs through its integrated components. Understanding these components helps you choose the right tools for specific data processing tasks.

```mermaid
flowchart TB
    Core["Spark Core"] --> SQL["Spark SQL & DataFrames"]
    Core --> Streaming["Spark Structured Streaming"]
    Core --> MLlib["MLlib (Machine Learning)"]
    Core --> GraphX["GraphX (Graph Processing)"]
    
    subgraph "Languages"
    Scala
    Java
    Python
    R
    SQL
    end
    
    Languages --> Core
    
    style Core fill:#f5d6c0,stroke:#333,stroke-width:2px
    style SQL fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Streaming fill:#ffefc0,stroke:#333,stroke-width:2px
    style MLlib fill:#c0e8f5,stroke:#333,stroke-width:2px
    style GraphX fill:#e7d1dd,stroke:#333,stroke-width:2px
    style Languages fill:#f8f9fa,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
```

1. **Spark Core**: The foundation of the entire project. It provides:
   - Distributed task dispatching
   - Scheduling
   - Basic I/O functionalities
   - The RDD (Resilient Distributed Dataset) API, which was the primary programming abstraction in early Spark versions

2. **Spark SQL and DataFrames**: A module for structured data processing that provides:
   - A DataFrame API that's similar to pandas but distributed
   - SQL interface for querying data
   - Optimized execution through the Catalyst optimizer
   - Seamless integration with various data sources

3. **Spark Structured Streaming**: An extension of the DataFrame API that enables:
   - Processing of continuous data streams
   - Unified API for batch and streaming
   - End-to-end exactly-once guarantees
   - Event-time processing

4. **MLlib (Machine Learning Library)**: A scalable machine learning library that includes:
   - Common learning algorithms for classification, regression, clustering, etc.
   - Feature extraction and transformation utilities
   - Model evaluation and tuning tools
   - Pipeline construction and management

5. **GraphX**: A library for graph computation and graph-parallel computation, providing:
   - A graph processing API
   - A collection of graph algorithms
   - Tools for building custom graph algorithms

All these components are designed to work together seamlessly, allowing you to combine different processing paradigms in a single application.

### 1.3 RDDs, DataFrames, and Datasets

Spark offers multiple APIs for distributed data processing, each with different levels of abstraction and optimization. Understanding these APIs helps you choose the right approach for your specific use case.

```mermaid
flowchart TD
    API["Spark APIs Evolution"] --> RDD["RDDs (Low Level)"]  
    API --> DF["DataFrames (Mid Level)"] 
    API --> DS["Datasets (High Level)"] 
    
    RDD -->|Abstraction| DF -->|Abstraction| DS
    DS -->|Performance| DF -->|Performance| RDD
    
    subgraph "PySpark Focus"
    DF
    RDD
    end
    
    style API fill:#f5d6c0,stroke:#333,stroke-width:2px
    style RDD fill:#ffefc0,stroke:#333,stroke-width:2px
    style DF fill:#d1e7dd,stroke:#333,stroke-width:2px,stroke-width:4px
    style DS fill:#c0e8f5,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
    style PySpark fill:#f8f9fa,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
```

#### RDDs (Resilient Distributed Datasets)

RDDs were the original abstraction in Spark. They represent an immutable, distributed collection of objects that can be processed in parallel.

**Key characteristics of RDDs:**
- Low-level API with fine-grained control
- Type-safety at compile time (more relevant in Scala/Java)
- No built-in optimization engine
- Require explicit management of data schemas
- Support for custom partitioning and data placement

**Example of RDD operations:**

```python
# Creating and using RDDs
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd_squared = rdd.map(lambda x: x * x)
result = rdd_squared.collect()
print(result)  # [1, 4, 9, 16, 25]
```

#### DataFrames

DataFrames provide a higher-level abstraction built on top of RDDs. They represent distributed collections of data organized into named columns, similar to tables in a relational database.

**Key characteristics of DataFrames:**
- Schema-aware data organization
- Optimized execution through the Catalyst optimizer
- SQL query support
- Efficient memory usage with columnar storage
- APIs available in Python, Scala, Java, and R

**Example of DataFrame operations:**

```python
# Creating and using DataFrames
df = spark.createDataFrame([(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)], 
                           ["id", "name", "age"])
filtered_df = df.filter(df.age > 25).select("id", "name")
filtered_df.show()
# +---+-------+
# | id|   name|
# +---+-------+
# |  2|    Bob|
# |  3|Charlie|
# +---+-------+
```

#### Datasets

Datasets are an extension of the DataFrame API that provides type-safety and object-oriented programming interface. They're primarily used in Scala and Java, as Python doesn't support the Dataset API due to its dynamic typing nature.

**Key characteristics of Datasets:**
- Type-safety at compile time
- Object-oriented programming interface
- Optimized execution through the Catalyst optimizer
- Encoder-based serialization/deserialization

> **Note**: In PySpark, you'll primarily work with DataFrames, as the Dataset API isn't available in Python. However, understanding the Dataset concept is useful when reading Spark documentation or working with JVM languages.

#### Knowledge Check

> **Question**: What are the main advantages of DataFrames over RDDs in Spark?
> 
> **Answer**: DataFrames offer schema awareness, optimized execution through the Catalyst optimizer, better memory efficiency with columnar storage, and SQL query support. These advantages typically result in better performance and simpler code compared to RDDs.

### 1.4 Spark Applications and Execution Model

Understanding how Spark executes applications is crucial for writing efficient code and troubleshooting performance issues. Let's explore the journey of a Spark application from code to execution.

```mermaid
flowchart TB
    Code["PySpark Code"] --> Session["SparkSession"] --> Logical["Logical Plan"] --> Optimized["Optimized Logical Plan"] --> Physical["Physical Plan"] --> Execute["Execution"]
    
    subgraph "Driver Program"
    Session
    Logical
    Optimized
    Physical
    end
    
    subgraph "Cluster Execution"
    Execute --> Job["Jobs"] --> Stage["Stages"] --> Task["Tasks"]
    end
    
    style Code fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Session fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Logical fill:#ffefc0,stroke:#333,stroke-width:2px
    style Optimized fill:#ffefc0,stroke:#333,stroke-width:2px
    style Physical fill:#ffefc0,stroke:#333,stroke-width:2px
    style Execute fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Job fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Stage fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Task fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Driver fill:#f8f9fa,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
    style Cluster fill:#f8f9fa,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
```

#### Spark Application Lifecycle

1. **Application Submission**: When you submit a Spark application, a driver process is launched that runs your `main()` function.

2. **Resource Allocation**: The driver requests resources from the cluster manager to launch executors.

3. **Execution Planning**: 
   - When you perform operations on DataFrames, Spark builds a logical plan representing your data transformations.
   - The Catalyst optimizer transforms and optimizes this logical plan.
   - Spark converts the optimized logical plan into a physical plan with specific execution strategies.

4. **Task Execution**:
   - The driver breaks the physical plan into stages and tasks.
   - Tasks are distributed to executors for parallel execution.
   - Executors process their assigned data partitions and return results to the driver.

#### Key Execution Concepts

1. **Jobs**: A Spark job corresponds to a single action (like `collect()` or `count()`). Each job is broken down into stages that can be executed in parallel.

2. **Stages**: A stage is a set of tasks that can be executed without data movement (shuffling). Stages are separated by operations that require shuffling data across partitions, such as `groupBy()` or `join()`.

3. **Tasks**: Tasks are the smallest execution units in Spark. Each task processes data from a single partition and runs on a single executor core.

4. **Shuffle**: The process of redistributing data across partitions, often across nodes. Shuffles are expensive operations as they involve disk I/O, network I/O, and serialization.

```mermaid
sequenceDiagram
    participant Driver
    participant Executor1 as Executor 1
    participant Executor2 as Executor 2
    
    Driver->>Driver: User submits action
    Driver->>Driver: Create job
    Driver->>Driver: Divide job into stages
    Driver->>Driver: Create tasks for each stage
    
    Driver->>Executor1: Submit tasks
    Driver->>Executor2: Submit tasks
    
    Executor1->>Executor1: Execute tasks on partitions
    Executor2->>Executor2: Execute tasks on partitions
    
    Executor1-->>Driver: Return results
    Executor2-->>Driver: Return results
    
    Driver->>Driver: Combine results
```

#### Lazy Evaluation

One of the key concepts in Spark's execution model is lazy evaluation. Transformations on DataFrames (like `select()`, `filter()`, or `join()`) don't trigger computation immediately. Instead, they build up a lineage of operations that will be executed when an action (like `show()`, `collect()`, or `count()`) is called.

This approach allows Spark to:
- Optimize the entire chain of operations before execution
- Avoid unnecessary computations
- Combine multiple operations into efficient execution stages

**Example of lazy evaluation:**

```python
# These transformations build up the lineage but don't execute yet
df = spark.read.csv("data.csv", header=True, inferSchema=True)
filtered_df = df.filter(df.age > 25)
selected_df = filtered_df.select("name", "city")

# This action triggers the execution of all previous transformations
result = selected_df.collect()
```

#### Knowledge Check

> **Question**: Why is lazy evaluation beneficial in Spark?
> 
> **Answer**: Lazy evaluation allows Spark to optimize the entire chain of operations before execution, potentially combining multiple operations, eliminating unnecessary steps, and creating an efficient execution plan. This often results in significant performance improvements compared to eager evaluation.
## 2. Setting Up the Spark Environment

### 2.1 SparkSession and Configuration

The `SparkSession` is the entry point to using Spark in Python. It provides a unified interface for interacting with Spark functionality and allows you to configure various aspects of your Spark application. In Databricks Community Edition, a SparkSession is automatically created for you, but understanding how to configure and customize it is important for optimizing your data processing tasks.

```mermaid
flowchart LR
    User["User Code"] --> SparkSession
    
    subgraph "Spark Session"
    SparkSession --> SQL["SQL Context"]
    SparkSession --> Streaming["Streaming"]
    SparkSession --> Catalog["Catalog"]
    SparkSession --> Context["Spark Context"]
    end
    
    Context --> Driver["Driver Program"]
    Driver --> Execution["Distributed Execution"]
    
    style User fill:#f5d6c0,stroke:#333,stroke-width:2px
    style SparkSession fill:#d1e7dd,stroke:#333,stroke-width:2px,stroke-width:4px
    style SQL fill:#ffefc0,stroke:#333,stroke-width:2px
    style Streaming fill:#ffefc0,stroke:#333,stroke-width:2px
    style Catalog fill:#ffefc0,stroke:#333,stroke-width:2px
    style Context fill:#ffefc0,stroke:#333,stroke-width:2px
    style Driver fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Execution fill:#c0e8f5,stroke:#333,stroke-width:2px
```

#### Creating a SparkSession

In Databricks, a SparkSession is already available as the variable `spark`. However, understanding how to create one from scratch is valuable for local development or custom configurations:

```python
from pyspark.sql import SparkSession

# Create a basic SparkSession
spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .getOrCreate()

# Print the Spark version
print(f"Spark version: {spark.version}")
```

The `builder` pattern with method chaining makes it easy to configure various aspects of your SparkSession before creating it.

#### Configuring SparkSession

You can configure numerous parameters to optimize your Spark application for specific workloads:

```python
from pyspark.sql import SparkSession

# Create a more heavily configured SparkSession
spark = SparkSession.builder \
    .appName("ConfiguredSparkApp") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10m") \
    .getOrCreate()
```

#### Key Configuration Parameters

Here are some important configuration parameters that you might want to adjust:

1. **Memory Allocation**:
   - `spark.executor.memory`: Amount of memory to use per executor process
   - `spark.driver.memory`: Amount of memory to use for the driver process
   - `spark.memory.fraction`: Fraction of heap space used for execution and storage

2. **Concurrency**:
   - `spark.executor.cores`: Number of cores to use on each executor
   - `spark.default.parallelism`: Default number of partitions for RDDs
   - `spark.sql.shuffle.partitions`: Number of partitions for shuffled data

3. **Performance Tuning**:
   - `spark.sql.autoBroadcastJoinThreshold`: Maximum size for broadcast joins
   - `spark.sql.broadcastTimeout`: Timeout for broadcast joins
   - `spark.sql.adaptive.enabled`: Enable adaptive query execution

4. **I/O**:
   - `spark.sql.files.maxPartitionBytes`: Maximum partition size when reading files
   - `spark.sql.files.openCostInBytes`: Estimated cost to open a file

```mermaid
flowchart TB
    Config["Spark Configuration"] --> Memory["Memory Settings"]
    Config --> Compute["Compute Settings"]
    Config --> IO["I/O Settings"]
    Config --> Shuffle["Shuffle Settings"]
    
    Memory --> M1["spark.executor.memory"]
    Memory --> M2["spark.driver.memory"]
    Memory --> M3["spark.memory.fraction"]
    
    Compute --> C1["spark.executor.cores"]
    Compute --> C2["spark.default.parallelism"]
    
    IO --> I1["spark.sql.files.maxPartitionBytes"]
    IO --> I2["spark.sql.parquet.compression.codec"]
    
    Shuffle --> S1["spark.sql.shuffle.partitions"]
    Shuffle --> S2["spark.shuffle.service.enabled"]
    
    style Config fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Memory fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Compute fill:#ffefc0,stroke:#333,stroke-width:2px
    style IO fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Shuffle fill:#e7d1dd,stroke:#333,stroke-width:2px
```

### 2.2 Managing SparkContext

The `SparkContext` is a lower-level entry point to Spark that was the primary interface in earlier Spark versions. In modern Spark applications using DataFrames, you typically interact with the `SparkSession` rather than directly with the `SparkContext`. However, understanding the SparkContext is still important, as it's responsible for the core functionality of connecting to the Spark cluster and creating RDDs.

When you create a SparkSession, a SparkContext is automatically created for you and can be accessed through the `spark.sparkContext` attribute:

```python
# Access the SparkContext from a SparkSession
sc = spark.sparkContext

# Check SparkContext configuration
print(f"Master: {sc.master}")
print(f"App ID: {sc.applicationId}")
print(f"App Name: {sc.appName}")
```

#### Common SparkContext Operations

```python
# Create an RDD from a Python collection
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Create an RDD from a text file
lines_rdd = sc.textFile("path/to/textfile.txt")

# Set the log level
sc.setLogLevel("WARN")  # Options: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

# Access configuration
config_value = sc.getConf().get("spark.executor.memory")
```

### 2.3 Setting Runtime Parameters

Sometimes you might want to adjust Spark parameters during the execution of your application rather than at initialization. This can be useful for tuning specific operations or implementing dynamic configuration based on data characteristics.

```python
# Set configuration at runtime
spark.conf.set("spark.sql.shuffle.partitions", 200)

# Get configuration value
current_partitions = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Current shuffle partitions: {current_partitions}")

# Update multiple configurations
spark.conf.set("spark.executor.memory", "6g")
spark.conf.set("spark.dynamicAllocation.enabled", "true")
```

#### Session-Specific vs. Global Configuration

It's important to understand the difference between session-specific and global configuration:

1. **Session-specific configuration**: Changes made using `spark.conf.set()` affect only the current SparkSession and don't persist across applications.

2. **Global configuration**: Some Spark properties can only be set at SparkSession creation time and cannot be changed at runtime.

```mermaid
sequenceDiagram
    participant App as Application Code
    participant Session as SparkSession
    participant Context as SparkContext
    participant Executor as Executors
    
    App->>Session: Create SparkSession with initial configs
    Session->>Context: Initialize SparkContext
    Context->>Executor: Launch with initial configs
    
    App->>Session: spark.conf.set("runtime.param", "value")
    Session->>Session: Update session config
    
    Note right of Session: Some configs affect<br>only new operations
    
    App->>Session: Run DataFrame operation
    Session->>Executor: Execute with updated configs
```

### 2.4 Application Deployment Models

When working with Spark, there are several deployment models to consider, each with advantages for different use cases:

```mermaid
flowchart TD
    Deploy["Spark Deployment Models"] --> Local["Local Mode"]
    Deploy --> Standalone["Standalone Cluster"]
    Deploy --> YARN["YARN"]
    Deploy --> Mesos["Mesos"]
    Deploy --> K8S["Kubernetes"]
    Deploy --> DBX["Databricks"]
    
    Local --> LD["Development & Testing"]
    Standalone --> SD["Small to Medium Clusters"]
    YARN --> YD["Hadoop Integration"]
    Mesos --> MD["Multi-Tenant Clusters"]
    K8S --> KD["Cloud-Native Deployments"]
    DBX --> DD["Managed Spark Environment"]
    
    style Deploy fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Local fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Standalone fill:#ffefc0,stroke:#333,stroke-width:2px
    style YARN fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Mesos fill:#e7d1dd,stroke:#333,stroke-width:2px
    style K8S fill:#f8d7da,stroke:#333,stroke-width:2px
    style DBX fill:#cfe2ff,stroke:#333,stroke-width:2px,stroke-width:4px
```

1. **Local Mode**: Spark runs on a single machine, using threads to simulate a cluster. Ideal for development, testing, and small datasets.

   ```python
   # Create a local SparkSession
   spark_local = SparkSession.builder \
       .appName("LocalSparkApp") \
       .master("local[*]") \
       .getOrCreate()
   ```

2. **Standalone Cluster**: Spark's built-in cluster manager that provides a simple way to run Spark on a cluster.

   ```python
   # Connect to a standalone cluster
   spark_standalone = SparkSession.builder \
       .appName("StandaloneSparkApp") \
       .master("spark://master:7077") \
       .getOrCreate()
   ```

3. **YARN**: Hadoop's resource manager, which allows Spark to run alongside other workloads on a Hadoop cluster.

   ```python
   # Connect to YARN cluster (typically set through spark-submit)
   # spark-submit --master yarn --deploy-mode cluster app.py
   ```

4. **Mesos**: A general-purpose cluster manager that can run Spark and other applications.

   ```python
   # Connect to Mesos cluster
   spark_mesos = SparkSession.builder \
       .appName("MesosSparkApp") \
       .master("mesos://master:7077") \
       .getOrCreate()
   ```

5. **Kubernetes**: A container orchestration platform that can be used to deploy and manage Spark applications.

   ```python
   # Connect to Kubernetes cluster (typically set through spark-submit)
   # spark-submit --master k8s://https://kubernetes-master:443 ...
   ```

6. **Databricks**: A managed platform that provides an optimized Spark environment with additional features. 
   - You don't need to specify the master URL, as it's handled by the platform.
   - In Databricks Community Edition, a SparkSession is automatically created for you.

#### Knowledge Check

> **Question**: What are the key differences between creating a SparkSession in a standalone application versus using Databricks?
> 
> **Answer**: In a standalone application, you need to explicitly create a SparkSession, specify the master URL, and manage configurations manually. In Databricks, a SparkSession is automatically created for you, the cluster management is handled by the platform, and many optimizations are pre-configured. Additionally, Databricks provides a notebook environment integrated with the Spark session.
## 3. Working with DataFrames

### 3.1 Creating DataFrames from Various Sources

DataFrames are the backbone of data processing in modern Spark applications. They provide a structured way to work with data, combining the best aspects of SQL tables and programming language collections. There are multiple ways to create DataFrames in PySpark, depending on your data source and requirements.

```mermaid
flowchart TD
    Sources["DataFrame Sources"] --> Manual["Manual Creation"]
    Sources --> Files["Files"]
    Sources --> DB["Databases"]
    Sources --> Streams["Streaming"]
    Sources --> Existing["Existing RDDs/DataFrames"]
    
    Manual --> List["Python Lists/Dicts"]
    Manual --> Pandas["Pandas DataFrames"]
    
    Files --> CSV["CSV"]
    Files --> JSON["JSON"]
    Files --> Parquet["Parquet"]
    Files --> ORC["ORC"]
    Files --> Text["Text"]
    Files --> Avro["Avro"]
    
    DB --> JDBC["JDBC/SQL"]
    DB --> Hive["Hive Tables"]
    DB --> NoSQL["NoSQL Connectors"]
    
    style Sources fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Manual fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Files fill:#ffefc0,stroke:#333,stroke-width:2px
    style DB fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Streams fill:#e7d1dd,stroke:#333,stroke-width:2px
    style Existing fill:#cfe2ff,stroke:#333,stroke-width:2px
```

#### Creating DataFrames Manually

For small datasets or testing, you can create DataFrames directly from Python data structures:

```python
# From a list of tuples
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 40)]
column_names = ["name", "age"]
df1 = spark.createDataFrame(data, column_names)
df1.show()

# From a list of dictionaries
data_dict = [
    {"name": "Alice", "age": 25, "city": "New York"},
    {"name": "Bob", "age": 30, "city": "San Francisco"},
    {"name": "Charlie", "age": 35, "city": "Seattle"}
]
df2 = spark.createDataFrame(data_dict)
df2.show()

# From a pandas DataFrame
import pandas as pd
pandas_df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "scores": [[85, 90], [95, 85], [75, 80]]
})
spark_df = spark.createDataFrame(pandas_df)
spark_df.show()
```

#### Reading Data from Files

Spark can read data from various file formats. The DataFrameReader interface, accessed via `spark.read`, provides a unified way to load data:

```python
# Reading CSV files
df_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("path/to/data.csv")

# Alternative syntax for CSV
df_csv = spark.read.csv(
    "path/to/data.csv",
    header=True,
    inferSchema=True
)

# Reading JSON files
df_json = spark.read.json("path/to/data.json")

# Reading Parquet files (columnar format, highly efficient)
df_parquet = spark.read.parquet("path/to/data.parquet")

# Reading ORC files (optimized row columnar format)
df_orc = spark.read.orc("path/to/data.orc")

# Reading text files
df_text = spark.read.text("path/to/text.txt")
```

#### Reading from Databases

Spark can connect to various databases using JDBC or native connectors:

```python
# Reading from a JDBC source (e.g., MySQL, PostgreSQL)
df_jdbc = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/database") \
    .option("dbtable", "users") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# Reading from Hive tables
spark.sql("USE my_database")
df_hive = spark.sql("SELECT * FROM my_table")
```

#### File Reading Best Practices

When reading files, consider these best practices for better performance and reliability:

1. **Specify Schema Explicitly**: While `inferSchema=True` is convenient, it requires an additional pass over the data. For large datasets or production code, define the schema explicitly:

   ```python
   from pyspark.sql.types import StructType, StructField, StringType, IntegerType
   
   # Define schema
   schema = StructType([
       StructField("id", IntegerType(), False),
       StructField("name", StringType(), True),
       StructField("age", IntegerType(), True),
       StructField("city", StringType(), True)
   ])
   
   # Read with explicit schema
   df = spark.read.csv("path/to/data.csv", header=True, schema=schema)
   ```

2. **Handle Data Quality Issues**: Use options to handle common data quality problems:

   ```python
   df = spark.read.csv(
       "path/to/data.csv",
       header=True,
       schema=schema,
       mode="DROPMALFORMED",  # Drop malformed lines
       nullValue="NA",       # Treat "NA" as null
       dateFormat="yyyy-MM-dd"  # Specify date format
   )
   ```

3. **Partitioned Data**: When reading from partitioned directories, you can leverage partitioning for better performance:

   ```python
   # Read partitioned data (e.g., data/year=2023/month=01/day=01/...)
   df = spark.read.parquet("path/to/partitioned/data")
   
   # Read only specific partitions
   df_subset = spark.read.parquet("path/to/data/year=2023/month=01")
   ```

```mermaid
flowchart LR
    Raw["Raw Data"] --> Reader["DataFrame Reader"] --> Options["Reader Options"] --> Format["Data Format"] --> Schema["Schema Definition"] --> LoadExec["Load Execution"] --> DF["DataFrame"]
    
    style Raw fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Reader fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Options fill:#ffefc0,stroke:#333,stroke-width:2px
    style Format fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Schema fill:#e7d1dd,stroke:#333,stroke-width:2px
    style LoadExec fill:#cfe2ff,stroke:#333,stroke-width:2px
    style DF fill:#d1e7dd,stroke:#333,stroke-width:2px,stroke-width:4px
```

#### Knowledge Check

> **Question**: What are the advantages of using an explicit schema when reading data into a DataFrame compared to using schema inference?
> 
> **Answer**: Using an explicit schema improves performance by eliminating the need for Spark to scan the data to infer types. It also provides more control over data types, helps catch data quality issues earlier, and ensures consistency across multiple data loads.

### 3.2 Schema Definition and Inference

The schema defines the structure of your DataFrame, including column names, data types, and nullable properties. Understanding schema management is crucial for working effectively with DataFrames.

```mermaid
flowchart TD
    Schema["Schema Management"] --> Inference["Schema Inference"]
    Schema --> Explicit["Explicit Definition"]
    Schema --> Inspection["Schema Inspection"]
    Schema --> Evolution["Schema Evolution"]
    
    Inference --> Auto["Automatic Type Detection"]
    Inference --> Sampling["Based on Data Sampling"]
    
    Explicit --> Structure["StructType & StructField"]
    Explicit --> DDL["DDL-formatted String"]
    
    Inspection --> Print["printSchema()"]
    Inspection --> Access["schema Property"]
    
    Evolution --> Merge["Union & Merge"]
    Evolution --> Cast["Type Casting"]
    
    style Schema fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Inference fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Explicit fill:#ffefc0,stroke:#333,stroke-width:2px
    style Inspection fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Evolution fill:#e7d1dd,stroke:#333,stroke-width:2px
```

#### Schema Inference

Spark can automatically infer the schema of your data by sampling a portion of it:

```python
# Schema inference with CSV
df = spark.read.option("inferSchema", "true").csv("data.csv", header=True)

# Schema is automatically inferred for JSON and Parquet
df_json = spark.read.json("data.json")
df_parquet = spark.read.parquet("data.parquet")
```

While convenient, schema inference has limitations:
- It requires an extra pass over the data, which can be expensive for large datasets
- Inferred types might not always match your expectations
- Type inference can be inconsistent if your data has mixed types or outliers

#### Explicit Schema Definition

Defining schemas explicitly gives you more control and better performance:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Define schema using StructType and StructField
schema = StructType([
    StructField("id", IntegerType(), nullable=False),  # Non-nullable integer
    StructField("name", StringType(), nullable=True),  # Nullable string
    StructField("age", IntegerType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("hire_date", DateType(), nullable=True)
])

# Use the defined schema
df = spark.read.schema(schema).csv("data.csv", header=True)
```

Alternatively, you can define a schema using a DDL-formatted string, similar to SQL table definitions:

```python
# Define schema using a DDL string
schema_ddl = "id INT NOT NULL, name STRING, age INT, salary DOUBLE, hire_date DATE"

df = spark.read.schema(schema_ddl).csv("data.csv", header=True)
```

#### Inspecting Schema

To view the schema of an existing DataFrame:

```python
# Print schema in a tree format
df.printSchema()

# Access schema as an object
schema = df.schema
print(schema)

# Get field names and types programmatically
for field in df.schema.fields:
    print(f"Field name: {field.name}, Type: {field.dataType}, Nullable: {field.nullable}")
```

#### Data Types in Spark

Spark supports a rich set of data types, including primitive types and complex types:

```python
from pyspark.sql.types import *

# Primitive types
integer_field = StructField("count", IntegerType())
long_field = StructField("id", LongType())
float_field = StructField("weight", FloatType())
double_field = StructField("score", DoubleType())
string_field = StructField("name", StringType())
boolean_field = StructField("active", BooleanType())
date_field = StructField("created", DateType())
timestamp_field = StructField("updated", TimestampType())

# Complex types
array_field = StructField("tags", ArrayType(StringType()))
map_field = StructField("properties", MapType(StringType(), StringType()))

# Nested structure
address_struct = StructType([
    StructField("street", StringType()),
    StructField("city", StringType()),
    StructField("zipcode", StringType())
])
address_field = StructField("address", address_struct)
```

#### Schema Evolution

As your data evolves, you might need to handle schema changes:

```python
# Merging schemas (union of all fields)
df_combined = spark.read.option("mergeSchema", "true").parquet("directory_with_different_schemas/*")

# Adding columns
from pyspark.sql.functions import lit
df_with_new_column = df.withColumn("new_column", lit(None).cast(StringType()))

# Changing column types
from pyspark.sql.functions import col
df_with_changed_type = df.withColumn("amount", col("amount").cast(DoubleType()))
```

### 3.3 Basic DataFrame Operations

Once you have a DataFrame, you can perform various operations to explore, transform, and analyze your data. Let's look at the fundamental operations you'll use frequently.

```mermaid
flowchart TD
    Operations["DataFrame Operations"] --> Inspect["Inspection"]
    Operations --> Select["Selection"]
    Operations --> Filter["Filtering"]
    Operations --> Order["Ordering"]
    Operations --> Modify["Modification"]
    Operations --> Missing["Missing Data"]
    
    Inspect --> Show["show()"]
    Inspect --> Count["count()"]
    Inspect --> Describe["describe()"]
    
    Select --> Cols["select()"]
    Select --> Drop["drop()"]
    
    Filter --> Where["where()/filter()"]
    
    Order --> Sort["sort()/orderBy()"]
    
    Modify --> New["withColumn()"]
    Modify --> Rename["withColumnRenamed()"]
    
    Missing --> Drop1["dropna()"]
    Missing --> Fill["fillna()"]
    
    style Operations fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Inspect fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Select fill:#ffefc0,stroke:#333,stroke-width:2px
    style Filter fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Order fill:#e7d1dd,stroke:#333,stroke-width:2px
    style Modify fill:#cfe2ff,stroke:#333,stroke-width:2px
    style Missing fill:#f8d7da,stroke:#333,stroke-width:2px
```

#### Inspecting DataFrames

Before diving into transformations, it's often helpful to examine your data:

```python
# Display the first n rows
df.show()  # Default is 20 rows
df.show(5)  # Show 5 rows
df.show(10, False)  # Show 10 rows without truncating strings

# Get the number of rows
row_count = df.count()
print(f"DataFrame has {row_count} rows")

# Get basic statistics for numeric columns
df.describe().show()

# Show only the schema
df.printSchema()

# Display DataFrame as a pandas DataFrame (useful in notebooks for better formatting)
display(df.limit(10).toPandas())

# Preview the first few rows
df.head(5)  # Returns a list of Row objects

# Get column names
print(df.columns)
```

#### Selecting Columns

Often, you'll want to work with a subset of columns:

```python
# Select specific columns
df_selected = df.select("name", "age", "salary")

# Select columns with column expressions
from pyspark.sql.functions import col, expr
df_expr = df.select(
    col("name"),
    col("age") + 1,  # Compute age + 1
    expr("salary * 1.1")  # Using SQL expression to give 10% raise
)

# Select all columns
df_all = df.select("*")

# Select columns by position
df_pos = df.select(df.columns[0], df.columns[2])

# Drop columns
df_dropped = df.drop("address", "phone")

# Select nested fields (assuming address is a struct)
df_nested = df.select(
    "name",
    "age",
    "address.city",  # Select nested field
    "address.zipcode"
)
```

#### Filtering Rows

Filtering allows you to focus on specific subsets of your data:

```python
# Basic filtering
df_adults = df.filter(df.age >= 18)

# Alternative syntax
df_adults = df.filter("age >= 18")

# Multiple conditions
df_filtered = df.filter((df.age >= 18) & (df.age <= 65))

# Using SQL expressions
df_filtered = df.filter("age >= 18 AND age <= 65")

# Filter with OR
df_or = df.filter((df.city == "New York") | (df.city == "San Francisco"))

# Filter with IN
df_in = df.filter(df.city.isin("New York", "San Francisco", "Seattle"))

# Filter with LIKE
df_like = df.filter(df.name.like("%Smith%"))

# Filter null values
df_not_null = df.filter(df.email.isNotNull())
```

#### Sorting and Limiting

Ordering your data can be important for analysis or presentation:

```python
# Sort by a single column (ascending by default)
df_sorted = df.sort("age")

# Sort descending
from pyspark.sql.functions import desc
df_desc = df.sort(desc("age"))

# Sort by multiple columns
df_multi = df.sort("department", desc("salary"))

# Alternative orderBy syntax
df_ordered = df.orderBy("department", desc("salary"))

# Limit results
df_top_10 = df.sort(desc("salary")).limit(10)
```

#### Adding and Modifying Columns

Transforming your data often involves adding or modifying columns:

```python
# Add a constant column
from pyspark.sql.functions import lit
df_with_constant = df.withColumn("status", lit("Active"))

# Add a computed column
df_with_computed = df.withColumn("salary_monthly", df.salary / 12)

# Complex computation with expression
df_with_expr = df.withColumn("full_name", expr("concat(first_name, ' ', last_name)"))

# Update existing column
df_updated = df.withColumn("age", df.age + 1)  # Increment everyone's age

# Rename column
df_renamed = df.withColumnRenamed("salary", "annual_salary")

# Cast column to different type
df_casted = df.withColumn("salary", df.salary.cast("double"))
```

#### Handling Missing Data

Dealing with nulls and missing values is a common challenge in data processing:

```python
# Count null values in each column
from pyspark.sql.functions import count, when, isnan, col
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Drop rows with any null values
df_no_nulls = df.dropna()

# Drop rows if specific columns have nulls
df_no_null_names = df.dropna(subset=["name", "email"])

# Drop rows if all values are null
df_any_value = df.dropna(how="all")

# Fill null values with a constant
df_filled = df.fillna(0)  # Fill all numeric columns with 0

# Fill specific columns with different values
df_filled_specific = df.fillna({"age": 0, "name": "Unknown", "email": "no-email"})

# Fill null values with column statistics
from pyspark.sql.functions import mean
mean_age = df.select(mean(df.age)).first()[0]
df_filled_mean = df.fillna({"age": mean_age})
```

#### Knowledge Check

> **Question**: How would you filter a DataFrame to find rows where a person's age is between 25 and 35, and they live in either "New York" or "San Francisco"?
> 
> **Answer**: You could use:
> ```python
> df_filtered = df.filter((df.age >= 25) & (df.age <= 35) & df.city.isin("New York", "San Francisco"))
> 
> # Or using SQL expression style:
> df_filtered = df.filter("age >= 25 AND age <= 35 AND city IN ('New York', 'San Francisco')")
> ```
## 4. Data Manipulation with PySpark

### 4.1 Column Operations and Expressions

Manipulating data in PySpark often involves working with columns and expressions. PySpark provides a rich set of functions to transform and analyze column data effectively.

```mermaid
flowchart TD
    Column["Column Operations"] --> Arithmetic["Arithmetic Operations"]
    Column --> String["String Operations"]
    Column --> Date["Date/Time Operations"]
    Column --> Conditional["Conditional Operations"]
    Column --> Aggregate["Aggregate Functions"]
    Column --> Window["Window Functions"]
    
    Arithmetic --> Add["+ (Addition)"]
    Arithmetic --> Sub["- (Subtraction)"]
    Arithmetic --> Mul["* (Multiplication)"]
    Arithmetic --> Div["/ (Division)"]
    
    String --> Concat["concat()"]
    String --> Substring["substring()"]
    String --> Upper["upper()/lower()"]
    
    Date --> Extract["year(), month(), day()"]
    Date --> Format["date_format()"]
    Date --> Add1["date_add()"]
    
    Conditional --> When["when().otherwise()"]
    Conditional --> Case["case when expressions"]
    
    style Column fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Arithmetic fill:#d1e7dd,stroke:#333,stroke-width:2px
    style String fill:#ffefc0,stroke:#333,stroke-width:2px
    style Date fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Conditional fill:#e7d1dd,stroke:#333,stroke-width:2px
    style Aggregate fill:#cfe2ff,stroke:#333,stroke-width:2px
    style Window fill:#f8d7da,stroke:#333,stroke-width:2px
```

#### Importing Functions

PySpark provides a wide range of built-in functions through the `pyspark.sql.functions` module. Most column operations use these functions:

```python
# Import commonly used functions
from pyspark.sql.functions import (
    col, lit, expr,  # Basic column references and expressions
    concat, substring, lower, upper, trim,  # String functions
    year, month, dayofmonth, hour, minute, second,  # Date functions
    round, sqrt, abs,  # Math functions
    when, coalesce,  # Conditional functions
    count, sum, avg, min, max,  # Aggregate functions
    dense_rank, row_number, lead, lag  # Window functions
)
```

#### Working with Column Expressions

There are several ways to reference and manipulate columns in PySpark:

```python
# Different ways to reference columns
df.select(
    df.age,           # Object attribute notation
    df["name"],        # Dictionary notation
    col("salary"),     # Using col() function
    expr("department")  # Using expr() function
).show()

# Using functions with columns
df.select(
    col("first_name"),
    col("last_name"),
    concat(col("first_name"), lit(" "), col("last_name")).alias("full_name")
).show()
```

#### Arithmetic Operations

You can perform basic arithmetic operations on numeric columns:

```python
# Arithmetic operations
df.select(
    col("salary"),
    col("salary") + 1000,  # Addition
    col("salary") - 1000,  # Subtraction
    col("salary") * 1.1,   # Multiplication
    col("salary") / 12,    # Division
    col("salary") % 1000,  # Modulo
).show()

# Using mathematical functions
from pyspark.sql.functions import round, sqrt, pow, abs

df.select(
    col("value"),
    round(col("value"), 2),  # Round to 2 decimal places
    sqrt(col("value")),      # Square root
    pow(col("value"), 2),     # Value squared
    abs(col("value"))        # Absolute value
).show()
```

#### String Operations

PySpark provides numerous functions for string manipulation:

```python
# String functions
from pyspark.sql.functions import (
    concat, concat_ws, substring, length, 
    upper, lower, trim, ltrim, rtrim,
    regexp_replace, regexp_extract
)

df.select(
    col("name"),
    upper(col("name")),           # Convert to uppercase
    lower(col("name")),           # Convert to lowercase
    length(col("name")),          # String length
    substring(col("name"), 1, 3),  # Extract substring (1-based indexing)
    concat(col("name"), lit(" Jr.")),  # Concatenate strings
    # Concatenate with separator
    concat_ws(" ", col("first_name"), col("middle_name"), col("last_name")),
    # Regular expression replacement
    regexp_replace(col("text"), "[0-9]+", "NUMBER"),
    # Regular expression extraction
    regexp_extract(col("email"), "(\\w+)@(\\w+)\\.(\\w+)", 1).alias("email_user")
).show()
```

#### Date and Time Operations

Working with dates and timestamps is common in data processing:

```python
# Date functions
from pyspark.sql.functions import (
    current_date, current_timestamp, date_format,
    year, month, dayofmonth, dayofweek,
    date_add, date_sub, datediff, months_between
)

df.select(
    col("date_column"),
    current_date().alias("today"),   # Current date
    current_timestamp().alias("now"),  # Current timestamp
    year(col("date_column")),        # Extract year
    month(col("date_column")),       # Extract month
    dayofmonth(col("date_column")),  # Extract day
    dayofweek(col("date_column")),   # Day of week (1 = Sunday, 2 = Monday, ...)
    # Format date as string
    date_format(col("date_column"), "yyyy-MM-dd").alias("formatted_date"),
    date_add(col("date_column"), 7).alias("next_week"),  # Add days
    date_sub(col("date_column"), 7).alias("last_week"),  # Subtract days
    datediff(current_date(), col("date_column")).alias("days_ago"),  # Days between
    months_between(current_date(), col("date_column")).alias("months_ago")  # Months between
).show()
```

#### Conditional Expressions

Conditional logic allows for more complex transformations:

```python
# Conditional functions
from pyspark.sql.functions import when, coalesce

# Using when/otherwise for if-then-else logic
df.select(
    col("age"),
    when(col("age") < 18, "Minor")
    .when(col("age") < 65, "Adult")
    .otherwise("Senior").alias("age_group")
).show()

# Multiple conditions with when
df.select(
    col("salary"),
    when(col("salary") < 50000, "Low")
    .when(col("salary") < 100000, "Medium")
    .when(col("salary") < 150000, "High")
    .otherwise("Very High").alias("salary_bracket")
).show()

# coalesce - returns first non-null value
df.select(
    col("primary_email"),
    col("secondary_email"),
    col("backup_email"),
    coalesce(col("primary_email"), col("secondary_email"), col("backup_email"), lit("no-email")).alias("contact_email")
).show()
```

#### Complex Expressions with expr()

The `expr()` function allows you to write SQL-like expressions as strings:

```python
# Using expr for complex expressions
df.select(
    col("first_name"),
    col("last_name"),
    expr("concat(first_name, ' ', last_name)").alias("full_name"),
    expr("case when age < 18 then 'Minor' when age < 65 then 'Adult' else 'Senior' end").alias("age_group"),
    expr("salary * 0.2").alias("bonus")
).show()
```

#### Knowledge Check

> **Question**: What's the difference between using column operations with functions like `col()` versus using SQL expressions with `expr()`?
> 
> **Answer**: Both approaches accomplish similar goals but with different syntax. The `col()` approach offers a more programmatic, type-safe way to work with columns and is often preferred for complex operations. The `expr()` approach uses SQL-like string expressions, which can be more concise and familiar to those with SQL experience. Both can be mixed in the same query, choosing whichever is more readable for the specific operation.

### 4.2 Filtering and Projection

Filtering (selecting rows) and projection (selecting columns) are fundamental operations in data processing. While we've touched on these earlier, let's explore more advanced techniques.

```mermaid
flowchart LR
    DF1["Original DataFrame"] --> Filter["Filter (Row Selection)"] --> Project["Projection (Column Selection)"] --> DF2["Resulting DataFrame"]
    
    subgraph "Filter Operations"
    F1["df.filter()"] 
    F2["df.where()"] 
    end
    
    subgraph "Projection Operations"
    P1["df.select()"] 
    P2["df.drop()"] 
    end
    
    style DF1 fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Filter fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Project fill:#ffefc0,stroke:#333,stroke-width:2px
    style DF2 fill:#c0e8f5,stroke:#333,stroke-width:2px
```

#### Advanced Filtering

```python
# Advanced filtering techniques
from pyspark.sql.functions import col, array_contains, isnull, to_date

# Filter with complex conditions
df_filtered = df.filter(
    (col("age") > 25) & 
    (col("department").isin("Engineering", "Marketing")) & 
    (col("salary") >= 50000)
)

# Filter with array containment
df_tagged = df.filter(array_contains(col("tags"), "python"))

# Filter based on null values in multiple columns
df_complete = df.filter(~isnull("email") & ~isnull("phone"))

# Filter based on date ranges
start_date = to_date(lit("2023-01-01"))
end_date = to_date(lit("2023-12-31"))
df_2023 = df.filter((col("hire_date") >= start_date) & (col("hire_date") <= end_date))

# Filter with regular expressions
df_gmail = df.filter(col("email").rlike("@gmail\\.com$"))
```

#### Advanced Projection

```python
# Advanced projection techniques
from pyspark.sql.functions import col, struct, array

# Select with dynamic columns
columns_to_select = ["id", "name", "email"]
df_selected = df.select(*columns_to_select)

# Select all columns matching a pattern
name_cols = [c for c in df.columns if c.endswith("_name")]
df_names = df.select("id", *name_cols)

# Select with column renaming
df_renamed = df.select(
    col("id"),
    col("first_name").alias("fname"),
    col("last_name").alias("lname")
)

# Creating struct columns (nested structures)
df_nested = df.select(
    "id",
    struct("first_name", "last_name").alias("name"),
    struct("street", "city", "state", "zip").alias("address")
)

# Creating array columns
df_array = df.select(
    "id",
    array("skill1", "skill2", "skill3").alias("skills")
)
```

### 4.3 Joins and Unions

Combining data from multiple DataFrames is a common requirement in data processing. PySpark provides various methods to join and union DataFrames.

```mermaid
flowchart TD
    subgraph "Join Types"
    Inner["Inner Join"] 
    Left["Left Outer Join"] 
    Right["Right Outer Join"] 
    Full["Full Outer Join"] 
    Cross["Cross Join"] 
    end
    
    subgraph "Set Operations"
    Union["Union"] 
    Intersect["Intersect"] 
    Except["Except"] 
    end
    
    DF1["DataFrame 1"] --> Inner & Left & Full & Cross & Union & Intersect & Except
    DF2["DataFrame 2"] --> Inner & Right & Full & Cross & Union & Intersect & Except
    
    style DF1 fill:#f5d6c0,stroke:#333,stroke-width:2px
    style DF2 fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Inner fill:#ffefc0,stroke:#333,stroke-width:2px
    style Left fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Right fill:#e7d1dd,stroke:#333,stroke-width:2px
    style Full fill:#cfe2ff,stroke:#333,stroke-width:2px
    style Cross fill:#f8d7da,stroke:#333,stroke-width:2px
    style Union fill:#f5f5dc,stroke:#333,stroke-width:2px
    style Intersect fill:#ffdab9,stroke:#333,stroke-width:2px
    style Except fill:#d8bfd8,stroke:#333,stroke-width:2px
```

#### Join Operations

Joins combine rows from two DataFrames based on related columns:

```python
# Sample DataFrames for joins
employees = spark.createDataFrame([
    (1, "Alice", "Engineering", 100),
    (2, "Bob", "Marketing", 200),
    (3, "Charlie", "Sales", 300),
    (4, "Diana", "Engineering", 400)
], ["id", "name", "department", "dept_id"])

departments = spark.createDataFrame([
    (100, "Engineering", "Building A"),
    (200, "Marketing", "Building B"),
    (300, "Sales", "Building C"),
    (500, "HR", "Building D")
], ["dept_id", "dept_name", "location"])

# Inner join (default)
inner_join = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "inner"
)

# Left outer join
left_join = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "left"
)

# Right outer join
right_join = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "right"
)

# Full outer join
full_join = employees.join(
    departments,
    employees.dept_id == departments.dept_id,
    "full"
)

# Cross join (Cartesian product)
cross_join = employees.crossJoin(departments)

# Join with column selection
join_select = employees.join(
    departments,
    employees.dept_id == departments.dept_id
).select(
    employees.id,
    employees.name,
    departments.dept_name,
    departments.location
)

# Join with multiple conditions
multi_condition_join = employees.join(
    departments,
    (employees.dept_id == departments.dept_id) & 
    (employees.department == departments.dept_name),
    "inner"
)
```

#### Join Strategies and Best Practices

Joins can be expensive operations, especially with large datasets. Here are some strategies to optimize joins:

```python
# Broadcast join for small tables
from pyspark.sql.functions import broadcast

# Explicitly broadcast the smaller table
broadcast_join = employees.join(
    broadcast(departments),
    employees.dept_id == departments.dept_id
)

# Filter before join to reduce data size
filtered_join = employees.filter(col("department") == "Engineering").join(
    departments.filter(col("location") == "Building A"),
    employees.dept_id == departments.dept_id
)

# Use appropriate join type
# If you only need matching records, use inner join instead of outer joins
# If one table is much larger, put it on the left side of a left join
```

#### Set Operations

PySpark provides set operations to combine or compare DataFrames with compatible schemas:

```python
# Sample DataFrames for set operations
team1 = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie")
], ["id", "name"])

team2 = spark.createDataFrame([
    (2, "Bob"),
    (3, "Charlie"),
    (4, "Diana")
], ["id", "name"])

# Union (combines all rows, keeps duplicates)
union_df = team1.union(team2)

# Union all (alias for union in PySpark)
union_all_df = team1.unionAll(team2)

# Union by name (matches columns by name, not position)
union_by_name_df = team1.unionByName(team2)

# Distinct union (removes duplicates)
distinct_union_df = team1.union(team2).distinct()

# Intersect (only rows in both DataFrames)
intersect_df = team1.intersect(team2)

# Except/Subtract (rows in first DataFrame but not in second)
except_df = team1.exceptAll(team2)  # keeps duplicates
subtract_df = team1.subtract(team2)  # equivalent to except
```

#### Knowledge Check

> **Question**: When would you choose to use a broadcast join over a regular join in PySpark?
> 
> **Answer**: Use a broadcast join when one of your DataFrames is significantly smaller than the other (typically less than a few hundred MB). In a broadcast join, the smaller DataFrame is sent to all executor nodes, avoiding the need for expensive data shuffling. This can dramatically improve performance for joins where one table is small enough to fit in memory.

### 4.4 Grouping and Aggregation

Grouping and aggregation are essential for summarizing and analyzing data. PySpark provides powerful functions for these operations.

```mermaid
flowchart LR
    DF1["Original DataFrame"] --> Group["Grouping"] --> Agg["Aggregation"] --> DF2["Aggregated DataFrame"]
    
    subgraph "Grouping"
    G1["df.groupBy()"] 
    end
    
    subgraph "Aggregation Functions"
    A1["count(), sum(), avg()"]
    A2["min(), max(), mean()"]
    A3["first(), last()"]
    A4["countDistinct()"]
    A5["Custom Aggregations"]
    end
    
    style DF1 fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Group fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Agg fill:#ffefc0,stroke:#333,stroke-width:2px
    style DF2 fill:#c0e8f5,stroke:#333,stroke-width:2px
```

#### Basic Grouping and Aggregation

```python
# Sample DataFrame for aggregation examples
employee_data = spark.createDataFrame([
    ("Engineering", "Alice", 100000, "NY"),
    ("Engineering", "Bob", 90000, "CA"),
    ("Engineering", "Carol", 95000, "CA"),
    ("Marketing", "Dave", 85000, "NY"),
    ("Marketing", "Eve", 80000, "CA"),
    ("Sales", "Frank", 70000, "NY"),
    ("Sales", "Grace", 75000, "CA"),
    ("Sales", "Heidi", 73000, "NY")
], ["department", "name", "salary", "state"])

# Group by one column with a single aggregation
dept_count = employee_data.groupBy("department").count()

# Group by one column with multiple aggregations
from pyspark.sql.functions import count, sum, avg, min, max, round

dept_stats = employee_data.groupBy("department").agg(
    count("*").alias("employee_count"),
    round(sum("salary"), 2).alias("total_salary"),
    round(avg("salary"), 2).alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary")
)

# Group by multiple columns
location_dept_stats = employee_data.groupBy("state", "department").agg(
    count("*").alias("employee_count"),
    round(avg("salary"), 2).alias("avg_salary")
)
```

#### Advanced Aggregation Functions

```python
# More advanced aggregation functions
from pyspark.sql.functions import (
    count, countDistinct, sum, avg, 
    min, max, first, last,
    collect_list, collect_set, array_sort
)

# Count distinct values
distinct_states = employee_data.groupBy("department").agg(
    countDistinct("state").alias("num_states")
)

# First and last values within group
first_last = employee_data.groupBy("department").agg(
    first("name").alias("first_employee"),
    last("name").alias("last_employee")
)

# Collect values into arrays
collect_values = employee_data.groupBy("department").agg(
    collect_list("name").alias("all_employees"),
    collect_set("state").alias("unique_states"),
    array_sort(collect_list("salary")).alias("salaries_sorted")
)
```

#### Pivot Operations

Pivot operations transform row values into columns, creating a cross-tabulation of your data:

```python
# Pivot example - count of employees by department and state
dept_state_pivot = employee_data.groupBy("department").pivot("state").count()

# Pivot with explicit values and multiple aggregations
from pyspark.sql.functions import sum, avg

salary_pivot = employee_data.groupBy("department") \
    .pivot("state", ["NY", "CA"]) \
    .agg(
        count("*").alias("count"),
        round(sum("salary"), 2).alias("total")
    )

# Unpivot (melt) - transforming columns back to rows
# First create a temporary view to use SQL for unpivot
dept_state_pivot.createOrReplaceTempView("pivoted_data")

# Use stack function to unpivot
unpivoted = spark.sql("""
    SELECT department, stack(2, 
        'NY', NY,
        'CA', CA
    ) as (state, employee_count)
    FROM pivoted_data
""")
```

#### Filtering After Aggregation

You can filter groups based on aggregate values:

```python
# Find departments with average salary over 90000
high_salary_depts = employee_data.groupBy("department").agg(
    avg("salary").alias("avg_salary")
).filter(col("avg_salary") > 90000)

# Find states with at least 3 employees
populated_states = employee_data.groupBy("state").agg(
    count("*").alias("employee_count")
).filter(col("employee_count") >= 3)

# Find departments in all states (having count of distinct states = total states)
total_states = employee_data.select("state").distinct().count()

depts_in_all_states = employee_data.groupBy("department").agg(
    countDistinct("state").alias("state_count")
).filter(col("state_count") == total_states)
```

#### Knowledge Check

> **Question**: How would you find the average salary for each department, but only include departments that have employees in both NY and CA states?
> 
> **Answer**: You can approach this in two ways:
> 
> ```python
> # First, find departments present in both states
> depts_in_both = employee_data.groupBy("department").agg(
>     countDistinct("state").alias("state_count")
> ).filter(col("state_count") >= 2)
> 
> # Then join with the aggregated data
> dept_avg = employee_data.groupBy("department").agg(
>     avg("salary").alias("avg_salary")
> )
> 
> result = dept_avg.join(depts_in_both, "department")
> ```
> 
> Or more concisely:
> 
> ```python
> # Using having-like logic
> from pyspark.sql.functions import collect_set
> 
> result = employee_data.groupBy("department").agg(
>     avg("salary").alias("avg_salary"),
>     collect_set("state").alias("states")
> ).filter(array_contains(col("states"), "NY") & array_contains(col("states"), "CA"))
> ```
## 5. Spark SQL and Query Optimization

### 5.1 Writing SQL Queries in Spark

Spark SQL provides a powerful interface for structured data processing using SQL syntax. This feature makes Spark accessible to data analysts and engineers already familiar with SQL, while still leveraging Spark's distributed processing capabilities.

```mermaid
flowchart TD
    DF["DataFrame API"] <--> SQL["SQL Interface"]
    
    SQL --> Temp["Temporary Views"]
    SQL --> Catalog["Catalog"] --> Perm["Permanent Tables"]
    SQL --> Query["SQL Queries"]
    
    Temp --> Select["SELECT Queries"]
    Query --> Select
    Perm --> Select
    
    Select --> Plan["Logical Plan"]
    Plan --> Optimized["Optimized Plan"]
    Optimized --> Execution["Execution"]
    
    style DF fill:#d1e7dd,stroke:#333,stroke-width:2px
    style SQL fill:#f5d6c0,stroke:#333,stroke-width:2px,stroke-width:4px
    style Temp fill:#ffefc0,stroke:#333,stroke-width:2px
    style Catalog fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Perm fill:#e7d1dd,stroke:#333,stroke-width:2px
    style Query fill:#cfe2ff,stroke:#333,stroke-width:2px
    style Select fill:#f8d7da,stroke:#333,stroke-width:2px
    style Plan fill:#f5f5dc,stroke:#333,stroke-width:2px
    style Optimized fill:#ffdab9,stroke:#333,stroke-width:2px
    style Execution fill:#d8bfd8,stroke:#333,stroke-width:2px
```

#### Creating Temporary Views

To use SQL with DataFrames, first register them as temporary views:

```python
# Sample DataFrame
employees = spark.createDataFrame([
    (1, "Alice", "Data Science", 90000, "NY"),
    (2, "Bob", "Engineering", 85000, "CA"),
    (3, "Charlie", "Data Science", 120000, "WA"),
    (4, "Diana", "Engineering", 110000, "CA"),
    (5, "Eva", "Marketing", 95000, "NY")
], ["id", "name", "department", "salary", "state"])

# Create a temporary view
employees.createOrReplaceTempView("employees")

# Create a global temporary view (visible across SparkSessions)
employees.createOrReplaceGlobalTempView("global_employees")
```

#### Basic SQL Queries

Once you've created a temporary view, you can run SQL queries using `spark.sql()` method:

```python
# Simple SELECT query
result = spark.sql("""
    SELECT name, department, salary 
    FROM employees 
    WHERE salary > 90000
""")
result.show()

# SQL query with aggregation
dept_stats = spark.sql("""
    SELECT 
        department, 
        COUNT(*) as employee_count, 
        ROUND(AVG(salary), 2) as avg_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""")
dept_stats.show()

# SQL query with JOIN
# First create another table
departments = spark.createDataFrame([
    ("Engineering", "Building A", "Jane Smith"),
    ("Data Science", "Building B", "Mike Johnson"),
    ("Marketing", "Building C", "Sarah Williams")
], ["dept_name", "location", "manager"])

departments.createOrReplaceTempView("departments")

# JOIN query
join_result = spark.sql("""
    SELECT e.name, e.department, d.location, d.manager
    FROM employees e
    JOIN departments d ON e.department = d.dept_name
    ORDER BY e.salary DESC
""")
join_result.show()
```

#### Advanced SQL Features

Spark SQL supports many advanced SQL features:

```python
# Common Table Expressions (CTEs)
cte_query = spark.sql("""
    WITH high_salary_employees AS (
        SELECT * FROM employees WHERE salary > 100000
    ),
    dept_counts AS (
        SELECT department, COUNT(*) as count FROM high_salary_employees GROUP BY department
    )
    
    SELECT e.department, e.name, e.salary, dc.count as high_earners_in_dept
    FROM employees e
    JOIN dept_counts dc ON e.department = dc.department
    ORDER BY e.salary DESC
""")

# Window functions
window_query = spark.sql("""
    SELECT 
        name, 
        department, 
        salary,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
        RANK() OVER (ORDER BY salary DESC) as overall_rank
    FROM employees
""")

# Subqueries
subquery = spark.sql("""
    SELECT name, department, salary
    FROM employees
    WHERE salary > (
        SELECT AVG(salary) FROM employees
    )
    ORDER BY salary DESC
""")

# CASE statements
case_query = spark.sql("""
    SELECT 
        name, 
        department,
        salary,
        CASE 
            WHEN salary > 100000 THEN 'High'
            WHEN salary > 90000 THEN 'Medium'
            ELSE 'Standard'
        END as salary_bracket
    FROM employees
""")
```

#### Working with Global Temporary Views

Global temporary views are visible across multiple SparkSessions within the same Spark application:

```python
# Access a global temporary view (note the special "global_temp" database prefix)
global_emp = spark.sql("SELECT * FROM global_temp.global_employees")

# Create a new SparkSession and access the global view
new_session = spark.newSession()
global_from_new = new_session.sql("SELECT * FROM global_temp.global_employees")

# Regular temporary views are NOT visible in the new session
try:
    new_session.sql("SELECT * FROM employees").show()
except Exception as e:
    print(f"Error: {e}")
```

#### SQL Functions and User-Defined Functions (UDFs)

You can use built-in SQL functions or create your own User-Defined Functions (UDFs):

```python
# Using built-in SQL functions
sql_functions = spark.sql("""
    SELECT 
        name,
        UPPER(name) as name_upper,
        LENGTH(name) as name_length,
        CONCAT(name, ' (', department, ')') as name_dept,
        YEAR(CURRENT_DATE) - 1 as last_year
    FROM employees
""")

# Register a Python function as a UDF
from pyspark.sql.types import StringType

@udf(StringType())
def generate_email(name, domain="company.com"):
    """Generate an email address from a name."""
    clean_name = name.lower().replace(" ", ".")
    return f"{clean_name}@{domain}"

# Register the UDF for SQL
spark.udf.register("generate_email", generate_email)

# Use the UDF in SQL
email_query = spark.sql("""
    SELECT 
        name, 
        department,
        generate_email(name) as email
    FROM employees
""")
```

#### Knowledge Check

> **Question**: What's the difference between a temporary view and a global temporary view in Spark SQL?
> 
> **Answer**: A regular temporary view is only available to the SparkSession that created it, and it disappears when the session ends. A global temporary view is available to all SparkSessions in the same Spark application and persists until the application ends. Global temporary views are accessed with the "global_temp" database prefix (e.g., `SELECT * FROM global_temp.view_name`).

### 5.2 Temporary Views and Tables

Understanding the differences between temporary views, global temporary views, and persistent tables is important for effectively working with Spark SQL.

```mermaid
flowchart TD
    SparkCatalog["Spark Catalog"] --> Temp["Temporary Views"]
    SparkCatalog --> Global["Global Temporary Views"]
    SparkCatalog --> Managed["Managed Tables"]
    SparkCatalog --> External["External Tables"]
    
    subgraph "Lifecycle"
    LS["Session Scope"] --- LA["Application Scope"] --- LP["Persistent Scope"]
    end
    
    Temp --- LS
    Global --- LA
    Managed --- LP
    External --- LP
    
    style SparkCatalog fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Temp fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Global fill:#ffefc0,stroke:#333,stroke-width:2px
    style Managed fill:#c0e8f5,stroke:#333,stroke-width:2px
    style External fill:#e7d1dd,stroke:#333,stroke-width:2px
    style Lifecycle fill:#f8f9fa,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5
```

#### Temporary Views

Temporary views are the most common way to work with DataFrames in SQL. They exist only for the duration of the SparkSession:

```python
# Create a temporary view
df.createOrReplaceTempView("my_temp_view")

# Query the view
result = spark.sql("SELECT * FROM my_temp_view WHERE value > 100")

# Check if a view exists
exists = spark.catalog.tableExists("my_temp_view")

# List all temporary views
temp_views = spark.catalog.listTables()
for view in temp_views:
    print(f"Name: {view.name}, Type: {view.tableType}, IsTemporary: {view.isTemporary}")

# Drop a temporary view when no longer needed
spark.catalog.dropTempView("my_temp_view")
```

#### Global Temporary Views

Global temporary views are visible to all SparkSessions in the same application and are stored in a special `global_temp` database:

```python
# Create a global temporary view
df.createOrReplaceGlobalTempView("my_global_view")

# Query the global view (note the global_temp prefix)
result = spark.sql("SELECT * FROM global_temp.my_global_view")

# Access from another session
new_session = spark.newSession()
result2 = new_session.sql("SELECT * FROM global_temp.my_global_view")

# List global temporary views
global_views = spark.catalog.listTables("global_temp")

# Drop a global temporary view
spark.catalog.dropGlobalTempView("my_global_view")
```

#### Persistent Tables

For data that needs to persist beyond the Spark application, you can create permanent tables. Spark supports two types of persistent tables:

1. **Managed Tables**: Spark manages both the metadata and data files. When a managed table is dropped, both metadata and data are deleted.

2. **External Tables**: Spark manages only the metadata; the data remains in the external location even if the table is dropped.

```python
# Create a managed table
df.write.saveAsTable("my_managed_table")

# Create an external table
df.write.option("path", "/path/to/external/storage").saveAsTable("my_external_table")

# Query a persistent table
result = spark.sql("SELECT * FROM my_managed_table")

# List all persistent tables in the default database
tables = spark.catalog.listTables()
for table in tables:
    if not table.isTemporary:
        print(f"Persistent table: {table.name}, Type: {table.tableType}")

# Drop a persistent table
spark.sql("DROP TABLE my_managed_table")
```

#### Database Operations

Spark SQL supports database operations for organizing tables:

```python
# Create a new database
spark.sql("CREATE DATABASE IF NOT EXISTS my_database")

# Set the current database
spark.sql("USE my_database")

# List all databases
databases = spark.catalog.listDatabases()
for db in databases:
    print(f"Database: {db.name}, Description: {db.description}")

# Create a table in a specific database
df.write.saveAsTable("my_database.my_table")

# Query a table in a specific database
result = spark.sql("SELECT * FROM my_database.my_table")

# Drop a database (must be empty or use CASCADE)
spark.sql("DROP DATABASE my_database")
# Force drop with all tables
spark.sql("DROP DATABASE my_database CASCADE")
```

### 5.3 Query Plans and Optimization

One of Spark's key strengths is its ability to optimize queries. Understanding query plans and the Catalyst optimizer can help you write more efficient queries.

```mermaid
flowchart TD
    SQL["SQL Query / DataFrame Operations"] --> LP["Logical Plan"]
    LP --> Catalyst["Catalyst Optimizer"]
    Catalyst --> OLP["Optimized Logical Plan"]
    OLP --> PP["Physical Plan"]
    PP --> EP["Execution Plan"]
    EP --> Results["Results"]
    
    Catalyst --> Rules["Optimization Rules"]
    Rules --> PushDown["Predicate Pushdown"]
    Rules --> Prune["Column Pruning"]
    Rules --> Combine["Combine Filters"]
    Rules --> Reorder["Join Reordering"]
    
    style SQL fill:#f5d6c0,stroke:#333,stroke-width:2px
    style LP fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Catalyst fill:#ffefc0,stroke:#333,stroke-width:2px,stroke-width:4px
    style OLP fill:#c0e8f5,stroke:#333,stroke-width:2px
    style PP fill:#e7d1dd,stroke:#333,stroke-width:2px
    style EP fill:#cfe2ff,stroke:#333,stroke-width:2px
    style Results fill:#f8d7da,stroke:#333,stroke-width:2px
    style Rules fill:#f5f5dc,stroke:#333,stroke-width:2px
```

#### Query Execution Process

When you execute a query, whether through SQL or the DataFrame API, Spark follows these steps:

1. **Parse**: Convert SQL or DataFrame operations into a logical plan (tree of logical operators)
2. **Analyze**: Resolve references, validate schema, etc.
3. **Optimize**: Apply optimization rules to improve the logical plan
4. **Physical Planning**: Convert logical plan to physical execution plan
5. **Execute**: Run the physical plan on the cluster

#### Examining Query Plans

Spark provides several methods to examine query plans:

```python
# Create a query to analyze
query = employees.filter(col("salary") > 90000) \
    .join(departments, col("department") == col("dept_name")) \
    .groupBy("location") \
    .agg(avg("salary").alias("avg_salary")) \
    .orderBy("avg_salary", ascending=False)

# View the logical plan
print("Logical Plan:")
print(query.explain())

# View the full execution plan (logical, optimized, and physical)
print("\nFull Plan:")
print(query.explain(True))

# Run SQL query and view its plan
sql_query = spark.sql("""
    SELECT location, AVG(salary) AS avg_salary
    FROM employees e
    JOIN departments d ON e.department = d.dept_name
    WHERE salary > 90000
    GROUP BY location
    ORDER BY avg_salary DESC
""")
sql_query.explain(True)
```

#### Catalyst Optimizer

The Catalyst optimizer applies various rules to transform the logical plan into a more efficient form:

1. **Predicate Pushdown**: Filters are pushed down to the data source to reduce the amount of data read.

2. **Column Pruning**: Only required columns are read from the data source.

3. **Constant Folding**: Expressions with constants are evaluated during optimization.

4. **Join Reordering**: Joins are reordered to minimize the amount of data processed.

5. **Partition Pruning**: For partitioned data, only relevant partitions are read.

#### AQE (Adaptive Query Execution)

Adaptive Query Execution, introduced in Spark 3.0, dynamically reoptimizes plans during execution:

```python
# Enable AQE (enabled by default in Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# AQE dynamically adjusts join strategies based on runtime statistics
spark.conf.set("spark.sql.adaptive.join.enabled", "true")

# AQE can coalesce shuffled partitions
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# AQE can optimize skewed joins
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

#### Query Plan Optimization Example

Let's look at how Spark optimizes a simple query:

```python
# Create two DataFrames to demonstrate optimization
customers = spark.createDataFrame([
    (1, "Alice", "NY"),
    (2, "Bob", "CA"),
    (3, "Charlie", "TX")
], ["customer_id", "name", "state"])

orders = spark.createDataFrame([
    (101, 1, 100.0, "2023-01-15"),
    (102, 2, 200.0, "2023-01-20"),
    (103, 1, 150.0, "2023-02-10"),
    (104, 3, 300.0, "2023-02-15")
], ["order_id", "customer_id", "amount", "order_date"])

# Register as temp views
customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")

# Write a query with optimization opportunities
query = spark.sql("""
    SELECT c.name, c.state, SUM(o.amount) as total_amount
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE o.amount > 100.0 AND c.state != 'TX'
    GROUP BY c.name, c.state
""")

# Explain the optimized query
query.explain(True)
```

In this example, we can observe several optimizations:

1. Filters (`amount > 100.0` and `state != 'TX'`) are pushed down to the source tables.
2. Only necessary columns (name, state, customer_id from customers; customer_id, amount from orders) are loaded.
3. Join strategy is selected based on table sizes.

#### Knowledge Check

> **Question**: What is "predicate pushdown" in Spark SQL optimization, and why is it beneficial?
> 
> **Answer**: Predicate pushdown is an optimization where filter conditions (predicates) are moved as close as possible to the data source before data is loaded into memory. This is beneficial because it significantly reduces the amount of data that needs to be read, transferred over the network, and processed by Spark. For example, if you're querying a large dataset but only need rows where `date > '2023-01-01'`, Spark can instruct the data source to filter rows at read time rather than loading all data first and then filtering.
## 6. Working with Complex Data Types

### 6.1 Arrays, Maps, and Structs

Real-world data often contains complex nested structures like arrays, maps, and structs. PySpark provides powerful capabilities for working with these complex data types, allowing you to manipulate and query nested data efficiently.

```mermaid
flowchart TD
    ComplexTypes["Complex Data Types"] --> Arrays["Arrays"]
    ComplexTypes --> Maps["Maps"]
    ComplexTypes --> Structs["Structs"]
    ComplexTypes --> Nesting["Nested Combinations"]
    
    Arrays --> AF["Array Functions"]
    Maps --> MF["Map Functions"]
    Structs --> SF["Struct Functions"]
    
    AF --> AC["array_contains()"]
    AF --> AE["explode()"]
    AF --> AZ["zip_with()"]
    
    MF --> MK["map_keys()"]
    MF --> MV["map_values()"]
    MF --> ME["explode()"]
    
    SF --> SD["getField()"]
    SF --> SN["Dot Notation"]
    
    style ComplexTypes fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Arrays fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Maps fill:#ffefc0,stroke:#333,stroke-width:2px
    style Structs fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Nesting fill:#e7d1dd,stroke:#333,stroke-width:2px
```

#### Creating DataFrames with Complex Types

Let's start by creating DataFrames that contain various complex data types:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, \
    ArrayType, MapType, DoubleType, BooleanType
from pyspark.sql.functions import col, struct, array, map_from_entries, lit

# Sample data with complex types
student_data = [
    (1, "Alice", [85, 90, 95], {"math": 85, "english": 90, "science": 95}, 
     ("123 Main St", "New York", "NY", "10001")),
    (2, "Bob", [75, 80, 85], {"math": 75, "english": 80, "science": 85}, 
     ("456 Oak Ave", "San Francisco", "CA", "94107")),
    (3, "Charlie", [90, 85, 80], {"math": 90, "english": 85, "science": 80}, 
     ("789 Pine St", "Seattle", "WA", "98101"))
]

# Define schema with complex types
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("scores", ArrayType(IntegerType()), True),
    StructField("subjects", MapType(StringType(), IntegerType()), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True)
    ]), True)
])

# Create DataFrame with complex types
students_df = spark.createDataFrame(student_data, schema)
students_df.printSchema()
students_df.show(truncate=False)

# Alternative: creating complex types using DataFrame operations
simple_data = [
    (1, "Alice", 85, 90, 95, "123 Main St", "New York", "NY", "10001"),
    (2, "Bob", 75, 80, 85, "456 Oak Ave", "San Francisco", "CA", "94107"),
    (3, "Charlie", 90, 85, 80, "789 Pine St", "Seattle", "WA", "98101")
]

simple_df = spark.createDataFrame(simple_data, 
    ["id", "name", "math", "english", "science", "street", "city", "state", "zip"])

# Create array column
simple_df = simple_df.withColumn(
    "scores", array(col("math"), col("english"), col("science"))
)

# Create map column
simple_df = simple_df.withColumn(
    "subjects", 
    map_from_entries(
        array(
            struct(lit("math"), col("math")),
            struct(lit("english"), col("english")),
            struct(lit("science"), col("science"))
        )
    )
)

# Create struct column
simple_df = simple_df.withColumn(
    "address",
    struct(col("street"), col("city"), col("state"), col("zip"))
)

# Select only the columns we need
complex_df = simple_df.select("id", "name", "scores", "subjects", "address")
complex_df.printSchema()
complex_df.show(truncate=False)
```

#### Working with Array Columns

Arrays in Spark represent collections of elements of the same type. Here are common operations with array columns:

```python
from pyspark.sql.functions import array_contains, explode, size, sort_array, \
    array_distinct, array_except, array_intersect, array_union, array_join, slice, element_at

# Check if array contains a value
students_df.select(
    "name", 
    "scores",
    array_contains("scores", 85).alias("has_85_score")
).show()

# Get array length
students_df.select(
    "name", 
    "scores",
    size("scores").alias("num_scores")
).show()

# Sort array
students_df.select(
    "name", 
    "scores",
    sort_array("scores").alias("sorted_scores"),
    sort_array("scores", asc=False).alias("sorted_desc")
).show()

# Get specific element (1-based indexing)
students_df.select(
    "name", 
    "scores",
    element_at("scores", 1).alias("first_score"),
    element_at("scores", -1).alias("last_score")  # Negative index for counting from end
).show()

# Get array slice
students_df.select(
    "name", 
    "scores",
    slice("scores", 1, 2).alias("first_two_scores")
).show()

# Join array elements into string
students_df.select(
    "name", 
    "scores",
    array_join("scores", ", ").alias("scores_string")
).show()

# Explode array (transform array elements to rows)
students_df.select(
    "name",
    explode("scores").alias("individual_score")
).show()

# Explode with position
from pyspark.sql.functions import posexplode
students_df.select(
    "name",
    posexplode("scores").alias("position", "individual_score")
).show()

# Collect scores across students
from pyspark.sql.functions import collect_list, collect_set
spark.sql("""
    SELECT 
        collect_list(scores) as all_scores_with_duplicates,
        collect_set(scores) as unique_score_arrays
    FROM students
""").show(truncate=False)

# Flatten nested arrays with flattenSchema
from pyspark.sql.functions import flatten

# Create nested array example
nestedArrayDF = spark.createDataFrame(
    [("A", [[1, 2, 3], [4, 5, 6]]), ("B", [[7, 8, 9], [10, 11]])],
    ["id", "nested_array"]
)

# Flatten the nested array
nestedArrayDF.select(
    "id",
    "nested_array",
    flatten("nested_array").alias("flattened_array")
).show(truncate=False)
```

#### Working with Map Columns

Maps represent key-value pairs, similar to Python dictionaries. Here's how to work with map columns:

```python
from pyspark.sql.functions import map_keys, map_values, explode_outer, map_from_arrays

# Extract keys and values from map
students_df.select(
    "name",
    "subjects",
    map_keys("subjects").alias("subject_names"),
    map_values("subjects").alias("subject_scores")
).show(truncate=False)

# Get value for a specific key
students_df.select(
    "name",
    "subjects",
    col("subjects.math").alias("math_score"),  # Get map value using dot notation
    element_at(col("subjects"), "english").alias("english_score")  # Alternative syntax
).show()

# Check if map contains a key
from pyspark.sql.functions import map_contains_key
students_df.select(
    "name",
    "subjects",
    map_contains_key(col("subjects"), "history").alias("has_history_score")
).show()

# Create map from separate arrays
key_value_df = spark.createDataFrame([
    ("Alice", ["math", "english", "science"], [85, 90, 95]),
    ("Bob", ["math", "english", "science"], [75, 80, 85])
], ["name", "subjects", "scores"])

key_value_df.select(
    "name",
    map_from_arrays(col("subjects"), col("scores")).alias("subject_map")
).show(truncate=False)

# Explode map to rows (convert key-value pairs to rows)
students_df.select(
    "name",
    explode("subjects").alias("subject", "score")
).show()
```

#### Working with Struct Columns

Structs in Spark represent nested records with named fields. Here's how to work with struct columns:

```python
from pyspark.sql.functions import struct

# Access struct fields using dot notation
students_df.select(
    "name",
    "address.street",
    "address.city",
    "address.state",
    "address.zip"
).show()

# Alternative: Access using getField
students_df.select(
    "name",
    col("address").getField("street").alias("street_address"),
    col("address").getField("city").alias("city_name")
).show()

# Create new struct from individual fields
students_df.select(
    "name",
    struct(
        col("address.street").alias("street_address"),
        col("address.city").alias("city_name"),
        col("address.state"),
        col("address.zip")
    ).alias("formatted_address")
).show(truncate=False)

# Filter based on struct field
students_df.filter(col("address.state") == "CA").show()

# Update nested field in struct
from pyspark.sql.functions import expr

updated_address = students_df.withColumn(
    "address",
    expr("named_struct('street', address.street, 'city', address.city, 'state', address.state, 'zip', '00000')")
)
updated_address.show(truncate=False)

# Alternative approach using withField (Spark 3.1+)
# updated_address = students_df.withColumn(
#     "address",
#     col("address").withField("zip", lit("00000"))
# )
```

### 6.2 JSON Processing

JSON is a common format for semi-structured data. Spark provides excellent support for working with JSON data, whether it's parsing JSON strings or processing entire JSON files.

```mermaid
flowchart TD
    JSON["JSON Processing"] --> Read["Reading JSON"] 
    JSON --> Write["Writing JSON"]
    JSON --> Parse["Parsing JSON Strings"]
    JSON --> Schema["Schema Inference"]
    
    Read --> RFile["Read JSON Files"]
    Read --> RStr["Read JSON Strings"]
    
    Parse --> FJ["from_json()"]
    Parse --> TJ["to_json()"]
    Parse --> Get["get_json_object()"]
    
    style JSON fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Read fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Write fill:#ffefc0,stroke:#333,stroke-width:2px
    style Parse fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Schema fill:#e7d1dd,stroke:#333,stroke-width:2px
```

#### Reading and Writing JSON

```python
# Reading JSON files
json_df = spark.read.json("path/to/data.json")

# Reading JSON with explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("skills", ArrayType(StringType()), True)
])

json_with_schema = spark.read.schema(json_schema).json("path/to/data.json")

# Writing DataFrame as JSON
df.write.json("path/to/output.json")

# Writing with options
df.write.mode("overwrite").option("compression", "gzip").json("path/to/output.json")
```

#### Parsing JSON Strings

Often, you'll have JSON stored as strings within a DataFrame column. Here's how to work with that:

```python
from pyspark.sql.functions import from_json, to_json, schema_of_json, get_json_object, json_tuple

# Sample DataFrame with JSON strings
json_string_data = [
    (1, "{\"name\": \"Alice\", \"age\": 25, \"skills\": [\"Python\", \"SQL\"]}"),
    (2, "{\"name\": \"Bob\", \"age\": 30, \"skills\": [\"Java\", \"Scala\"]}"),
    (3, "{\"name\": \"Charlie\", \"age\": 35, \"skills\": [\"Python\", \"Spark\"]}")
]
json_string_df = spark.createDataFrame(json_string_data, ["id", "json_data"])

# Infer schema from JSON strings
sample_json = json_string_df.select("json_data").first()[0]
inferred_schema = schema_of_json(lit(sample_json))
print(f"Inferred schema: {inferred_schema}")

# Define schema for JSON parsing
json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("skills", ArrayType(StringType()), True)
])

# Parse JSON strings to struct using from_json
parsed_df = json_string_df.withColumn(
    "parsed_data", 
    from_json(col("json_data"), json_schema)
)
parsed_df.show(truncate=False)

# Access fields from the parsed JSON
parsed_df.select(
    "id",
    "parsed_data.name",
    "parsed_data.age",
    "parsed_data.skills"
).show(truncate=False)

# Extract specific fields without parsing the entire JSON
extracted_df = json_string_df.select(
    "id",
    get_json_object(col("json_data"), "$.name").alias("name"),
    get_json_object(col("json_data"), "$.age").alias("age"),
    get_json_object(col("json_data"), "$.skills[0]").alias("first_skill")
)
extracted_df.show()

# Extract multiple fields at once with json_tuple
tupled_df = json_string_df.select(
    "id",
    json_tuple(col("json_data"), "name", "age").alias("name", "age")
)
tupled_df.show()

# Convert struct to JSON string
from_struct_df = parsed_df.select(
    "id",
    to_json(col("parsed_data")).alias("json_string")
)
from_struct_df.show(truncate=False)
```

### 6.3 Working with XML Data

XML is another common format for semi-structured data. While Spark doesn't include built-in XML support, you can use the `spark-xml` package to work with XML data.

```mermaid
flowchart TD
    XML["XML Processing"] --> Install["Install spark-xml"] 
    Install --> Read["Read XML"]
    Install --> Write["Write XML"]
    Read --> Options["Reading Options"]
    Write --> WOptions["Writing Options"]
    
    style XML fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Install fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Read fill:#ffefc0,stroke:#333,stroke-width:2px
    style Write fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Options fill:#e7d1dd,stroke:#333,stroke-width:2px
    style WOptions fill:#cfe2ff,stroke:#333,stroke-width:2px
```

#### Setting Up XML Support

```python
# Install the spark-xml package
# This can be done in Databricks by adding the Maven coordinates
# com.databricks:spark-xml_2.12:0.14.0 to the cluster configuration

# Alternatively, if you're using PySpark outside Databricks, you can include 
# the package when starting PySpark:
# pyspark --packages com.databricks:spark-xml_2.12:0.14.0
```

#### Reading and Writing XML

```python
# Reading XML files
xml_df = spark.read.format("xml") \
    .option("rowTag", "book") \
    .load("path/to/books.xml")

# Reading XML with options
xml_df = spark.read.format("xml") \
    .option("rowTag", "book") \
    .option("attributePrefix", "_") \
    .option("valueTag", "_VALUE") \
    .load("path/to/books.xml")

# Writing DataFrame as XML
df.write.format("xml") \
    .option("rootTag", "books") \
    .option("rowTag", "book") \
    .save("path/to/output.xml")
```

#### XML Processing Options

The `spark-xml` package offers many options for reading and writing XML:

**Reading Options:**
- `rowTag`: The XML tag name for rows (required)
- `samplingRatio`: Sampling ratio for schema inference (default: 1.0)
- `excludeAttribute`: Whether to exclude attributes (default: false)
- `attributePrefix`: Prefix for attributes (default: _)
- `valueTag`: Tag used for the value when a key has both attributes and a value (default: _VALUE)

**Writing Options:**
- `rootTag`: Root tag of the XML (default: rows)
- `rowTag`: Row tag of the XML (default: row)
- `declaration`: XML declaration (default: version="1.0" encoding="UTF-8")
- `nullValue`: Value to write for null fields (default: null)

#### Knowledge Check

> **Question**: How would you extract specific elements from an array column in a DataFrame and convert them to individual rows?
> 
> **Answer**: You would use the `explode()` function to transform array elements into rows. For example:
> ```python
> from pyspark.sql.functions import explode
> 
> # Assuming df has a column 'items' which is an array
> exploded_df = df.select(
>     "id",  # Keep other columns you need
>     explode("items").alias("single_item")
> )
> ```
> This will create a new row for each element in the 'items' array, paired with the corresponding 'id' value from the original row.
## 7. ETL Operations in Spark

### 7.1 Data Cleaning and Validation

Extract, Transform, Load (ETL) is a fundamental process in data engineering. It involves extracting data from various sources, transforming it to fit business needs, and loading it into a target data store. Spark is an excellent platform for implementing ETL processes because of its scalability and rich data processing capabilities.

```mermaid
flowchart LR
    Extract["Extract"] --> Transform["Transform"] --> Load["Load"]
    
    subgraph "Extract"
    E1["Read from sources"] 
    E2["Parse formats"] 
    E3["Filter initial data"] 
    end
    
    subgraph "Transform"
    T1["Clean"] 
    T2["Validate"] 
    T3["Standardize"] 
    T4["Enrich"] 
    T5["Aggregate"] 
    end
    
    subgraph "Load"
    L1["Format output"] 
    L2["Write to target"] 
    L3["Verify loading"] 
    end
    
    style Extract fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Transform fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Load fill:#ffefc0,stroke:#333,stroke-width:2px
    style E1 fill:#f8d7da,stroke:#333,stroke-width:2px
    style E2 fill:#f8d7da,stroke:#333,stroke-width:2px
    style E3 fill:#f8d7da,stroke:#333,stroke-width:2px
    style T1 fill:#cfe2ff,stroke:#333,stroke-width:2px
    style T2 fill:#cfe2ff,stroke:#333,stroke-width:2px
    style T3 fill:#cfe2ff,stroke:#333,stroke-width:2px
    style T4 fill:#cfe2ff,stroke:#333,stroke-width:2px
    style T5 fill:#cfe2ff,stroke:#333,stroke-width:2px
    style L1 fill:#fff3cd,stroke:#333,stroke-width:2px
    style L2 fill:#fff3cd,stroke:#333,stroke-width:2px
    style L3 fill:#fff3cd,stroke:#333,stroke-width:2px
```

Data cleaning is often the most time-consuming aspect of ETL. Real-world data is messy, with issues like missing values, duplicates, inconsistent formats, and outliers. Let's explore how to address these issues in Spark.

#### Handling Missing Values

```python
from pyspark.sql.functions import col, count, when, isnan, isnull, coalesce, lit

# Sample data with missing values
data = [
    (1, "Alice", 25, "NY", 50000),
    (2, "Bob", None, "CA", 60000),
    (3, "Charlie", 35, None, None),
    (4, None, 40, "TX", 70000),
    (5, "Eve", 30, "FL", None)
]

df = spark.createDataFrame(data, ["id", "name", "age", "state", "salary"])

# Identify missing values
def count_nulls(df):
    """Count null values in each column of a DataFrame"""
    return df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns])

count_nulls(df).show()

# Option 1: Drop rows with missing values
df_no_nulls = df.na.drop()  # Drop any row with any null value
df_no_nulls.show()

# Drop rows with null in specific columns only
df_required = df.na.drop(subset=["id", "name"])  # Keep only rows with id and name
df_required.show()

# Option 2: Fill missing values with constants
df_filled = df.na.fill({
    "age": 0,
    "state": "Unknown",
    "salary": 0,
    "name": "Unknown"
})
df_filled.show()

# Option 3: Fill with statistics or derived values
# Calculate average age and salary
avg_age = df.select("age").filter(col("age").isNotNull()).agg({"age": "avg"}).collect()[0][0]
avg_salary = df.filter(col("salary").isNotNull()).agg({"salary": "avg"}).collect()[0][0]

# Fill with calculated values
df_calculated = df.na.fill({
    "age": avg_age,
    "salary": avg_salary
})
df_calculated.show()

# Option 4: More advanced filling with expressions
df_advanced = df.withColumn(
    "age", 
    coalesce(col("age"), lit(avg_age))
).withColumn(
    "salary",
    coalesce(col("salary"), when(col("state") == "NY", 55000).otherwise(avg_salary))
)
df_advanced.show()
```

#### Removing Duplicates

```python
# Sample data with duplicates
dup_data = [
    (1, "Alice", "NY"),
    (2, "Bob", "CA"),
    (1, "Alice", "NY"),  # Exact duplicate
    (3, "Alice", "NY"),  # Partial duplicate (same name and state)
    (4, "Charlie", "TX"),
    (5, "Bob", "FL"),    # Partial duplicate (same name)
]

dup_df = spark.createDataFrame(dup_data, ["id", "name", "state"])
dup_df.show()

# Remove exact duplicates
df_distinct = dup_df.distinct()
df_distinct.show()

# Remove duplicates based on specific columns
df_dedup = dup_df.dropDuplicates(["name", "state"])
df_dedup.show()

# Keep first occurrence of duplicates
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Define window specification for partitioning by name and state, ordering by id
window_spec = Window.partitionBy("name", "state").orderBy("id")

# Add row number within each partition
df_with_row_num = dup_df.withColumn("row_num", row_number().over(window_spec))

# Keep only the first occurrence in each partition
df_first_occurrence = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
df_first_occurrence.show()
```

#### Standardizing Data

```python
from pyspark.sql.functions import trim, upper, lower, regexp_replace, translate, lpad, to_date

# Sample data with format inconsistencies
messy_data = [
    (1, " Alice ", "NY", "F", "2023/01/15"),
    (2, "bob", "ca", "MALE", "01-15-2023"),
    (3, "CHARLIE", "TX", "m", "2023.01.15"),
    (4, "Diana123", "FL", "female", "Jan 15, 2023"),
    (5, "Eve-Smith", "WA", "FEMALE", "15/01/2023")
]

messy_df = spark.createDataFrame(messy_data, ["id", "name", "state", "gender", "date"])
messy_df.show()

# Standardize text: trim spaces, consistent case
std_df = messy_df.withColumn(
    "name", 
    trim(col("name"))  # Remove leading/trailing spaces
).withColumn(
    "name",
    initcap(col("name"))  # Title case (first letter of each word uppercase)
).withColumn(
    "state",
    upper(col("state"))  # Uppercase state codes
)

# Standardize gender codes
std_df = std_df.withColumn(
    "gender_code",
    when(lower(col("gender")).isin("m", "male"), "M")
    .when(lower(col("gender")).isin("f", "female"), "F")
    .otherwise("U")  # Unknown
)

# Clean up names (remove non-alphabetic characters)
std_df = std_df.withColumn(
    "clean_name",
    regexp_replace(col("name"), "[^a-zA-Z ]", "")  # Remove non-alphabetic chars
)

# Standardize dates (convert various formats to standard date type)
std_df = std_df.withColumn(
    "standard_date",
    coalesce(
        to_date(col("date"), "yyyy/MM/dd"),
        to_date(col("date"), "MM-dd-yyyy"),
        to_date(col("date"), "yyyy.MM.dd"),
        to_date(col("date"), "MMM dd, yyyy"),
        to_date(col("date"), "dd/MM/yyyy")
    )
)

std_df.show()
```

#### Data Validation

Data validation ensures that data meets quality standards before proceeding with further processing.

```python
from pyspark.sql.functions import col, length, regexp_extract

# Sample data for validation
validation_data = [
    (1, "Alice", 25, "alice@example.com", "123-45-6789"),
    (2, "Bob", 17, "bob@example", "123-45-678"),
    (3, "Charlie", 135, "charlie@gmail.com", "123-45-6789"),
    (4, "Diana", 32, "diana.example.com", "12345-6789"),
    (5, "Eve", 29, "eve@example.com", "123-45-6789")
]

val_df = spark.createDataFrame(validation_data, 
                              ["id", "name", "age", "email", "ssn"])

# Define validation rules
is_valid_age = (col("age") >= 18) & (col("age") <= 120)
is_valid_email = col("email").rlike("^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$")
is_valid_ssn = col("ssn").rlike("^\\d{3}-\\d{2}-\\d{4}$")

# Add validation flags
validated_df = val_df.withColumn("valid_age", is_valid_age)
validated_df = validated_df.withColumn("valid_email", is_valid_email)
validated_df = validated_df.withColumn("valid_ssn", is_valid_ssn)

# Add overall validity flag
validated_df = validated_df.withColumn(
    "is_valid_record",
    is_valid_age & is_valid_email & is_valid_ssn
)

validated_df.show()

# Filter out invalid records
valid_records = validated_df.filter(col("is_valid_record"))
valid_records.show()

# Collect invalid records for reporting
invalid_records = validated_df.filter(~col("is_valid_record"))
invalid_records.show()

# Generate validation report
validation_report = validated_df.agg(
    count("*").alias("total_records"),
    sum(when(col("valid_age"), 1).otherwise(0)).alias("valid_age_count"),
    sum(when(col("valid_email"), 1).otherwise(0)).alias("valid_email_count"),
    sum(when(col("valid_ssn"), 1).otherwise(0)).alias("valid_ssn_count"),
    sum(when(col("is_valid_record"), 1).otherwise(0)).alias("valid_record_count")
)
validation_report.show()
```

#### Knowledge Check

> **Question**: What's the difference between using `df.na.drop()` and `df.dropDuplicates()` in a PySpark DataFrame?
> 
> **Answer**: `df.na.drop()` removes rows containing null/missing values, helping to clean data where missing values would cause problems in analysis. `df.dropDuplicates()` removes duplicate rows (exact matches across all columns by default, or matches on specified columns), which helps eliminate redundancy in the dataset. The two functions address different data quality issues and are often used together in data cleaning pipelines.
### 7.2 Data Transformation and Enrichment

Beyond cleaning and validating data, transformation involves reshaping, restructuring, and enriching data to make it suitable for analysis or downstream applications. Spark provides powerful functions for these transformation tasks.

```mermaid
flowchart TD
    subgraph "Data Transformation"
    RF["Reshape & Format"] --> RE["Restructure"]
    RE --> EV["Enrich & Validate"]
    end
    
    subgraph "Reshape & Format"
    R1["Type Conversion"] 
    R2["Field Generation"] 
    R3["String Manipulation"] 
    end
    
    subgraph "Restructure"
    S1["Pivot/Unpivot"] 
    S2["Denormalization"] 
    S3["Aggregation"] 
    end
    
    subgraph "Enrich & Validate"
    E1["Lookup Enrichment"] 
    E2["Derived Features"] 
    E3["Final Validation"] 
    end
    
    style RF fill:#d1e7dd,stroke:#333,stroke-width:2px
    style RE fill:#f5d6c0,stroke:#333,stroke-width:2px
    style EV fill:#c0e8f5,stroke:#333,stroke-width:2px
```

#### Reshaping and Type Transformations

```python
from pyspark.sql.functions import col, concat, concat_ws, substring, to_date, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, expr
from pyspark.sql.functions import round as spark_round, format_number

# Sample data
data = [
    (1, "John Doe", "1980-05-15", "New York", 75000.5678, "Engineer"),
    (2, "Jane Smith", "1992-10-28", "San Francisco", 85000.1234, "Data Scientist"),
    (3, "Bob Johnson", "1975-03-21", "Chicago", 92000.7541, "Manager"),
]

df = spark.createDataFrame(data, ["id", "name", "birth_date", "city", "salary", "title"])

# 1. Type conversions
transformed_df = df.withColumn(
    "birth_date", 
    to_date(col("birth_date"))  # Convert string to date
).withColumn(
    "salary", 
    col("salary").cast("double")  # Ensure salary is double type
)

# 2. Extract components from dates
transformed_df = transformed_df.withColumn(
    "birth_year", 
    year(col("birth_date"))
).withColumn(
    "birth_month", 
    month(col("birth_date"))
).withColumn(
    "birth_day", 
    dayofmonth(col("birth_date"))
)

# 3. String transformations
transformed_df = transformed_df.withColumn(
    "first_name", 
    expr("split(name, ' ')[0]")
).withColumn(
    "last_name", 
    expr("split(name, ' ')[1]")
).withColumn(
    "name_length", 
    length(col("name"))
)

# 4. Numerical transformations
transformed_df = transformed_df.withColumn(
    "salary_k", 
    (col("salary") / 1000).cast("integer")
).withColumn(
    "salary_formatted", 
    format_number(col("salary"), 2)  # Format with 2 decimal places
).withColumn(
    "salary_rounded", 
    spark_round(col("salary"), 0)  # Round to nearest integer
)

transformed_df.show()
```

#### Advanced Restructuring

```python
from pyspark.sql.functions import expr, collect_list, struct, col, explode

# Sample sales data
sales_data = [
    ("2023-01-15", "A", 100),
    ("2023-01-15", "B", 200),
    ("2023-01-16", "A", 120),
    ("2023-01-16", "B", 210),
    ("2023-01-17", "A", 130),
    ("2023-01-17", "B", 220)
]

sales_df = spark.createDataFrame(sales_data, ["date", "product", "revenue"])

# 1. Pivot data (convert rows to columns)
sales_pivot = sales_df.groupBy("date").pivot("product").sum("revenue")
sales_pivot.show()

# 2. Unpivot data (convert columns to rows)
# First create a temporary view to use SQL for unpivot
sales_pivot.createOrReplaceTempView("pivoted_sales")

# Use stack function to unpivot (melt) the data
unpivoted_df = spark.sql("""
    SELECT date, stack(2, 'A', A, 'B', B) as (product, revenue)
    FROM pivoted_sales
""")
unpivoted_df.show()

# 3. Collect values into arrays
grouped_df = sales_df.groupBy("date").agg(
    collect_list(
        struct("product", "revenue")
    ).alias("products_revenue")
)
grouped_df.show(truncate=False)

# 4. Explode arrays back to rows
exploded_df = grouped_df.select(
    "date",
    explode("products_revenue").alias("prod_rev")
).select(
    "date",
    "prod_rev.product",
    "prod_rev.revenue"
)
exploded_df.show()

# 5. Window functions for advanced restructuring
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, row_number, rank, dense_rank

# Define window specifications
window_by_product = Window.partitionBy("product").orderBy("date")
window_by_date = Window.partitionBy("date").orderBy(col("revenue").desc())

# Apply window functions
windowed_df = sales_df.withColumn(
    "previous_day_revenue", 
    lag("revenue", 1).over(window_by_product)
).withColumn(
    "next_day_revenue", 
    lead("revenue", 1).over(window_by_product)
).withColumn(
    "revenue_change", 
    col("revenue") - lag("revenue", 1).over(window_by_product)
).withColumn(
    "rank_in_day", 
    rank().over(window_by_date)
)

windowed_df.show()
```

#### Data Enrichment

Data enrichment involves adding value to your dataset by incorporating information from other sources or computing derived fields.

```python
from pyspark.sql.functions import col, when, broadcast, current_date, datediff, lit

# Main customer data
customer_data = [
    (1, "Alice", "NY", "2020-05-15"),
    (2, "Bob", "CA", "2019-10-10"),
    (3, "Charlie", "TX", "2021-02-25"),
    (4, "Diana", "FL", "2018-07-30"),
    (5, "Eve", "WA", "2022-01-05")
]

customer_df = spark.createDataFrame(customer_data, 
                                  ["customer_id", "name", "state", "signup_date"])

# Additional reference data for enrichment
state_region_data = [
    ("NY", "Northeast", 19.3),
    ("CA", "West", 12.1),
    ("TX", "South", 8.7),
    ("FL", "South", 7.8),
    ("IL", "Midwest", 6.2)
]

state_df = spark.createDataFrame(state_region_data, 
                               ["state", "region", "tax_rate"])

# 1. Lookup enrichment (joining with reference data)
# Use broadcast join for small reference data
enriched_df = customer_df.join(
    broadcast(state_df),
    on="state",
    how="left"
)

# 2. Add derived fields
final_df = enriched_df.withColumn(
    "signup_date", 
    to_date(col("signup_date"))
).withColumn(
    "customer_tenure_days", 
    datediff(current_date(), col("signup_date"))
).withColumn(
    "customer_tenure_years", 
    spark_round(col("customer_tenure_days") / 365, 1)
).withColumn(
    "region", 
    when(col("region").isNull(), "Other").otherwise(col("region"))
).withColumn(
    "tax_rate", 
    when(col("tax_rate").isNull(), 5.0).otherwise(col("tax_rate"))
).withColumn(
    "customer_segment", 
    when(col("customer_tenure_years") > 3, "Loyal")
    .when(col("customer_tenure_years") > 1, "Established")
    .otherwise("New")
)

final_df.show()
```

### 7.3 Data Loading and Output

The final stage of the ETL process is loading data into target systems. Spark can write data to various formats and destinations.

```mermaid
flowchart TD
    Loading["Data Loading"] --> Format["Output Format"]
    Loading --> Mode["Write Mode"]
    Loading --> Partn["Partitioning"]
    Loading --> Compr["Compression"]
    
    Format --> F1["CSV"] 
    Format --> F2["Parquet"] 
    Format --> F3["JSON"] 
    Format --> F4["Delta Lake"] 
    Format --> F5["JDBC"] 
    
    Mode --> M1["overwrite"] 
    Mode --> M2["append"] 
    Mode --> M3["ignore"] 
    Mode --> M4["error"] 
    
    Partn --> P1["Single Partition"] 
    Partn --> P2["Partitioned Columns"] 
    Partn --> P3["Bucketing"] 
    
    style Loading fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Format fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Mode fill:#ffefc0,stroke:#333,stroke-width:2px
    style Partn fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Compr fill:#e7d1dd,stroke:#333,stroke-width:2px
```

#### Writing to Different Formats

```python
# Sample data for writing examples
processed_data = [
    (1, "Product A", "Electronics", 299.99, "2023-01-15"),
    (2, "Product B", "Clothing", 49.99, "2023-01-15"),
    (3, "Product C", "Electronics", 199.99, "2023-01-16"),
    (4, "Product D", "Home", 129.99, "2023-01-16"),
    (5, "Product E", "Clothing", 79.99, "2023-01-17")
]

processed_df = spark.createDataFrame(
    processed_data, 
    ["product_id", "product_name", "category", "price", "date"]
)

# Convert date column to actual date type
processed_df = processed_df.withColumn("date", to_date(col("date")))

# 1. Write to CSV
processed_df.write.mode("overwrite") \
    .option("header", "true") \
    .csv("output/products_csv")

# 2. Write to Parquet (columnar format, more efficient)
processed_df.write.mode("overwrite") \
    .parquet("output/products_parquet")

# 3. Write to JSON
processed_df.write.mode("overwrite") \
    .json("output/products_json")

# 4. Write to ORC
processed_df.write.mode("overwrite") \
    .orc("output/products_orc")

# 5. Write with partitioning (organizing data by columns)
processed_df.write.mode("overwrite") \
    .partitionBy("category", "date") \
    .parquet("output/products_partitioned")

# 6. Write with bucketing (organizing data into buckets)
processed_df.write.mode("overwrite") \
    .bucketBy(4, "category") \
    .sortBy("product_id") \
    .saveAsTable("products_bucketed")

# 7. Write with compression
processed_df.write.mode("overwrite") \
    .option("compression", "gzip") \
    .csv("output/products_compressed_csv")

processed_df.write.mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("output/products_compressed_parquet")
```

#### Writing to Databases

```python
# Writing to a JDBC database (e.g., PostgreSQL)
processed_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydatabase") \
    .option("dbtable", "products") \
    .option("user", "username") \
    .option("password", "password") \
    .mode("overwrite") \
    .save()

# Writing to Hive tables
processed_df.write \
    .mode("overwrite") \
    .saveAsTable("default.products")
```

#### Managing Write Modes

Spark offers several write modes that determine how data is written when the destination already exists:

```python
# 1. Overwrite mode - replaces existing data
df.write.mode("overwrite").parquet("output/path")

# 2. Append mode - adds data to existing data
df.write.mode("append").parquet("output/path")

# 3. Ignore mode - ignores the write operation if data exists
df.write.mode("ignore").parquet("output/path")

# 4. Error mode (default) - throws an error if data exists
df.write.mode("error").parquet("output/path")
```

#### Partitioning Strategies

Partitioning can significantly improve query performance by organizing data in a way that allows Spark to skip irrelevant data during reads:

```python
# Basic partitioning by date
df.write.partitionBy("date").parquet("output/date_partitioned")

# Multi-level partitioning
df.write.partitionBy("year", "month", "day").parquet("output/multi_partitioned")

# Partitioning with custom directory structure
df = df.withColumn("year", year(col("date")))
df = df.withColumn("month", month(col("date")))
df.write.partitionBy("year", "month").parquet("output/custom_partitioned")
```

#### Knowledge Check

> **Question**: What's the difference between `partitionBy()` and `bucketBy()` when writing data in Spark?
> 
> **Answer**: `partitionBy()` organizes data into separate directories based on the values of specified columns, creating a hierarchical structure that allows Spark to skip entire partitions during queries. It's good for high-cardinality columns like date. `bucketBy()` organizes data within a table/directory into a fixed number of buckets based on a hash of the specified columns, which helps with joins and aggregations on the bucketed columns. Bucketing is better for columns with high cardinality where partitioning would create too many small files.
## 8. Batch Processing Patterns

Batch processing is a fundamental approach in data engineering where data is collected over time and processed as a batch. Spark excels at batch processing, and several common patterns have emerged as best practices in the industry.

```mermaid
flowchart TD
    Batch["Batch Processing Patterns"] --> Incr["Incremental Processing"]
    Batch --> Window["Windowed Processing"]
    Batch --> Partition["Partitioning Strategies"]
    Batch --> Error["Error Handling"]
    
    Incr --> I1["Full Refresh"] 
    Incr --> I2["Incremental Updates"] 
    Incr --> I3["Change Data Capture"] 
    
    Window --> W1["Time Windows"] 
    Window --> W2["Sliding Windows"] 
    Window --> W3["Session Windows"] 
    
    Partition --> P1["Hash Partitioning"] 
    Partition --> P2["Range Partitioning"] 
    Partition --> P3["Dynamic Partitioning"] 
    
    Error --> E1["Exception Handling"] 
    Error --> E2["Data Quality Checks"] 
    Error --> E3["Dead Letter Queue"] 
    
    style Batch fill:#f5d6c0,stroke:#333,stroke-width:2px
    style Incr fill:#d1e7dd,stroke:#333,stroke-width:2px
    style Window fill:#ffefc0,stroke:#333,stroke-width:2px
    style Partition fill:#c0e8f5,stroke:#333,stroke-width:2px
    style Error fill:#e7d1dd,stroke:#333,stroke-width:2px
```

### 8.1 Incremental Processing

Incremental processing is a pattern where only new or modified data is processed in each batch run, rather than processing the entire dataset every time. This approach can significantly improve efficiency for large datasets.

#### Full Refresh vs. Incremental Updates

```python
from pyspark.sql.functions import current_timestamp, lit, col

# Sample source data (imagine this is a snapshot of a data source)
source_data = [
    (1, "Product A", 10.0, "2023-01-15 10:00:00"),
    (2, "Product B", 20.0, "2023-01-15 11:00:00"),
    (3, "Product C", 30.0, "2023-01-15 12:00:00")
]

source_df = spark.createDataFrame(
    source_data, 
    ["id", "name", "price", "last_updated"]
)

# Save this as our "current state"
source_df.write.mode("overwrite").saveAsTable("products_current")

# Approach 1: Full Refresh Pattern
def full_refresh_process():
    """Process all data and replace the target completely."""
    # Read the entire source data
    source = spark.table("products_current")
    
    # Apply transformations
    transformed = source.withColumn("price_with_tax", col("price") * 1.1)
    
    # Write to target, overwriting existing data
    transformed.write.mode("overwrite").saveAsTable("products_processed")
    
    print("Full refresh completed")

# Approach 2: Incremental Update Pattern
def incremental_process(checkpoint_time):
    """Process only new or modified data since the last checkpoint."""
    # Read only new/modified records
    source = spark.table("products_current")
    new_records = source.filter(col("last_updated") > checkpoint_time)
    
    if new_records.count() == 0:
        print("No new data to process")
        return current_timestamp().cast("string")
    
    # Apply transformations
    transformed = new_records.withColumn("price_with_tax", col("price") * 1.1)
    
    # Merge with existing target data
    if spark.catalog.tableExists("products_processed"):
        # Append new records
        transformed.write.mode("append").saveAsTable("products_processed")
    else:
        # Create table if it doesn't exist
        transformed.write.mode("overwrite").saveAsTable("products_processed")
    
    # Return new checkpoint time
    return current_timestamp().cast("string")

# Simulate incremental processing
# 1. Initial load
checkpoint_time = "2023-01-01 00:00:00"
checkpoint_time = incremental_process(checkpoint_time)

# 2. Add new data to source
new_data = [
    (4, "Product D", 40.0, "2023-01-16 09:00:00"),
    (5, "Product E", 50.0, "2023-01-16 10:00:00")
]

new_df = spark.createDataFrame(
    new_data, 
    ["id", "name", "price", "last_updated"]
)

# Append to source
new_df.write.mode("append").saveAsTable("products_current")

# 3. Run incremental process again
checkpoint_time = incremental_process(checkpoint_time)

# Check results
result = spark.table("products_processed")
result.show()
```

#### Change Data Capture (CDC)

Change Data Capture is a more sophisticated incremental processing pattern that tracks all changes (inserts, updates, deletes) to source data.

```python
from pyspark.sql.functions import current_timestamp, lit, col, when

# Sample CDC data with operation type
cdc_data = [
    (1, "Product A", 10.0, "2023-01-16 09:00:00", "INSERT"),
    (2, "Product B", 25.0, "2023-01-16 10:00:00", "UPDATE"),  # Price changed
    (3, "Product C", 30.0, "2023-01-16 11:00:00", "DELETE")
]

cdc_df = spark.createDataFrame(
    cdc_data, 
    ["id", "name", "price", "operation_time", "operation_type"]
)

# Function to apply CDC changes to a target table
def apply_cdc_changes(cdc_data, target_table):
    """Apply CDC changes to target table using merge/upsert pattern."""
    # Check if target exists
    if not spark.catalog.tableExists(target_table):
        # Create target with only INSERTs if it doesn't exist
        inserts = cdc_data.filter(col("operation_type") == "INSERT")
        if inserts.count() > 0:
            inserts.drop("operation_type", "operation_time") \
                  .write.mode("overwrite").saveAsTable(target_table)
        return
    
    # Get current target data
    target = spark.table(target_table)
    
    # Process each operation type
    # 1. Process DELETEs
    deletes = cdc_data.filter(col("operation_type") == "DELETE").select("id")
    if deletes.count() > 0:
        # Remove deleted records
        target = target.join(deletes, "id", "left_anti")
    
    # 2. Process UPSERTs (INSERT and UPDATE)
    upserts = cdc_data.filter(col("operation_type").isin("INSERT", "UPDATE")) \
                     .drop("operation_type", "operation_time")
    
    if upserts.count() > 0:
        # Remove records that will be updated
        target = target.join(upserts.select("id"), "id", "left_anti")
        # Add new and updated records
        target = target.union(upserts)
    
    # Write back to target
    target.write.mode("overwrite").saveAsTable(target_table)

# Apply CDC changes
apply_cdc_changes(cdc_df, "products_target")

# Check results
result = spark.table("products_target")
result.show()
```

### 8.2 Windowed Processing

Windowed processing is a pattern where data is processed in defined time windows, allowing for time-based analysis and aggregations.

```python
from pyspark.sql.functions import window, col, sum, avg, count, to_timestamp
from pyspark.sql.window import Window

# Sample time-series data
time_data = [
    ("2023-01-01 08:30:00", "sensor1", 25.0),
    ("2023-01-01 08:45:00", "sensor1", 26.0),
    ("2023-01-01 09:00:00", "sensor1", 27.0),
    ("2023-01-01 09:15:00", "sensor1", 28.0),
    ("2023-01-01 09:30:00", "sensor1", 29.0),
    ("2023-01-01 08:30:00", "sensor2", 20.0),
    ("2023-01-01 08:45:00", "sensor2", 21.0),
    ("2023-01-01 09:00:00", "sensor2", 22.0),
    ("2023-01-01 09:15:00", "sensor2", 23.0),
    ("2023-01-01 09:30:00", "sensor2", 24.0)
]

time_df = spark.createDataFrame(
    time_data, 
    ["timestamp", "sensor_id", "temperature"]
)

# Convert string timestamp to actual timestamp
time_df = time_df.withColumn(
    "timestamp", 
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

# 1. Tumbling Window (fixed, non-overlapping windows)
tumbling_windows = time_df.groupBy(
    window(col("timestamp"), "30 minutes"),
    "sensor_id"
).agg(
    avg("temperature").alias("avg_temp"),
    count("*").alias("reading_count")
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "sensor_id",
    "avg_temp",
    "reading_count"
)

tumbling_windows.show()

# 2. Sliding Window (overlapping windows)
sliding_windows = time_df.groupBy(
    window(col("timestamp"), "30 minutes", "15 minutes"),  # 30-min window, sliding every 15 min
    "sensor_id"
).agg(
    avg("temperature").alias("avg_temp"),
    count("*").alias("reading_count")
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "sensor_id",
    "avg_temp",
    "reading_count"
)

sliding_windows.show()

# 3. Rolling Window (window function based)
window_spec = Window.partitionBy("sensor_id") \
                   .orderBy("timestamp") \
                   .rangeBetween(-30 * 60, 0)  # 30 minutes back from current row

rolling_df = time_df.withColumn(
    "rolling_avg_temp", 
    avg("temperature").over(window_spec)
).withColumn(
    "readings_in_window", 
    count("*").over(window_spec)
)

rolling_df.show()
```

### 8.3 Partitioning Strategies

Effective partitioning is critical for Spark performance, especially for large datasets. The right partitioning strategy can significantly reduce processing time.

```python
from pyspark.sql.functions import year, month, dayofmonth, spark_partition_id, hash, col

# Sample large dataset (simulated)
large_data = spark.range(0, 1000000) \
    .withColumn("timestamp", expr("date_add(to_date('2023-01-01'), id % 365)")) \
    .withColumn("value", expr("rand() * 1000")) \
    .withColumn("category", expr("concat('category_', cast(id % 10 as string))"))

# 1. Default partitioning
print(f"Default number of partitions: {large_data.rdd.getNumPartitions()}")

# Check partition distribution
large_data.groupBy(spark_partition_id()).count().show()

# 2. Repartition by number (balancing load)
repartitioned_df = large_data.repartition(20)
print(f"After repartition: {repartitioned_df.rdd.getNumPartitions()}")

# 3. Repartition by column (co-locating related data)
repart_by_category = large_data.repartition("category")
repart_by_category.groupBy("category", spark_partition_id()).count().show()

# 4. Repartition by multiple columns
repart_by_time_category = large_data.repartition(
    year("timestamp"), month("timestamp"), "category"
)

# 5. Custom partitioning using hash function
from pyspark.sql.functions import hash

# Create a custom hash for partitioning
custom_hash_df = large_data.withColumn(
    "partition_key", 
    hash(col("category"), month("timestamp")) % 20
).repartition("partition_key")

# 6. Coalesce to reduce partitions (does not shuffle data unless necessary)
coalesced_df = large_data.coalesce(10)
print(f"After coalesce: {coalesced_df.rdd.getNumPartitions()}")
```

### 8.4 Error Handling and Resilience

Production-grade batch processing requires robust error handling to ensure reliability and data quality.

```python
from pyspark.sql.functions import col, expr, lit, when
import datetime

# Sample data with potential errors
error_data = [
    (1, "Alice", "25", "NY", "2023-01-15"),
    (2, "Bob", "invalid_age", "CA", "2023-01-16"),
    (3, "Charlie", "35", "TX", "invalid_date"),
    (4, "Diana", None, "FL", "2023-01-18"),
    (5, "Eve", "30", None, "2023-01-19")
]

error_df = spark.createDataFrame(
    error_data, 
    ["id", "name", "age_str", "state", "date_str"]
)

# 1. Basic Exception Handling
def safe_cast_to_int(df, column):
    """Safely cast a string column to integer, setting errors to null."""
    return df.withColumn(
        f"{column}_int",
        when(
            col(column).rlike("^\\d+$"),  # Check if string is numeric
            col(column).cast("int")
        ).otherwise(None)
    )

def safe_cast_to_date(df, column):
    """Safely cast a string column to date, setting errors to null."""
    return df.withColumn(
        f"{column}_parsed",
        when(
            col(column).rlike("^\d{4}-\d{2}-\d{2}$"),  # Basic date format check
            to_date(col(column), "yyyy-MM-dd")
        ).otherwise(None)
    )

# Apply safe transformations
processed_df = error_df
processed_df = safe_cast_to_int(processed_df, "age_str")
processed_df = safe_cast_to_date(processed_df, "date_str")

# 2. Dead Letter Queue Pattern
# Separate valid and invalid records
valid_records = processed_df.filter(
    col("age_str_int").isNotNull() & 
    col("date_str_parsed").isNotNull() & 
    col("state").isNotNull()
)

invalid_records = processed_df.filter(
    col("age_str_int").isNull() | 
    col("date_str_parsed").isNull() | 
    col("state").isNull()
).withColumn(
    "error_timestamp", 
    current_timestamp()
).withColumn(
    "error_reason",
    when(col("age_str_int").isNull(), "Invalid age")
    .when(col("date_str_parsed").isNull(), "Invalid date")
    .when(col("state").isNull(), "Missing state")
    .otherwise("Unknown error")
)

# Process valid records
valid_results = valid_records.select(
    "id", "name", "age_str_int", "state", "date_str_parsed"
)

# Store invalid records for later investigation
invalid_records.write.mode("append").saveAsTable("error_records")

# 3. Monitoring and Logging
# Collect processing statistics
processing_stats = spark.createDataFrame([
    ("total_records", processed_df.count()),
    ("valid_records", valid_records.count()),
    ("invalid_records", invalid_records.count()),
    ("processing_time", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
], ["metric", "value"])

# Log stats or write to monitoring table
processing_stats.show()
```

### 8.5 End-to-End Batch Processing Example

Let's put these patterns together in a complete end-to-end example of a batch processing job:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, day, current_timestamp
from pyspark.sql.functions import when, lit, coalesce, count, sum, avg, max, min
import datetime

def batch_process(input_path, output_path, processing_date, checkpoint_path=None):
    """End-to-end batch processing job with best practices."""
    
    # Create or get SparkSession
    spark = SparkSession.builder \
        .appName("BatchProcessingJob") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    try:
        # 1. Extract: Load data incrementally if checkpoint exists
        if checkpoint_path:
            try:
                # Try to read last checkpoint
                checkpoint_df = spark.read.parquet(checkpoint_path)
                last_processed_date = checkpoint_df.first()["last_processed_date"]
                print(f"Incremental processing from {last_processed_date}")
            except:
                # No checkpoint found, process everything
                last_processed_date = "1900-01-01"
                print("Full processing (no checkpoint found)")
        else:
            # No checkpoint requested, process everything
            last_processed_date = "1900-01-01"
            print("Full processing (no checkpoint requested)")
        
        # Read input data
        input_df = spark.read.parquet(input_path)
        
        # Filter for incremental processing
        if last_processed_date != "1900-01-01":
            input_df = input_df.filter(col("date") > last_processed_date)
        
        # Short-circuit if no new data
        if input_df.isEmpty():
            print("No new data to process")
            return
        
        # 2. Validate: Separate valid and invalid records
        validated_df = input_df.withColumn(
            "is_valid", 
            col("customer_id").isNotNull() & 
            col("product_id").isNotNull() & 
            col("amount").isNotNull() & 
            (col("amount") > 0)
        )
        
        valid_records = validated_df.filter(col("is_valid") == True)
        invalid_records = validated_df.filter(col("is_valid") == False) \
            .withColumn("error_time", current_timestamp()) \
            .withColumn("processing_date", lit(processing_date))
        
        # Write invalid records to "dead letter queue"
        if invalid_records.count() > 0:
            invalid_records.write.mode("append") \
                .partitionBy("processing_date") \
                .parquet(f"{output_path}/errors")
        
        # 3. Transform: Apply business logic
        transformed_df = valid_records \
            .withColumn("date", to_date(col("date"))) \
            .withColumn("year", year(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("day", day(col("date"))) \
            .withColumn("amount_with_tax", col("amount") * 1.1) \
            .withColumn("processing_timestamp", current_timestamp())
        
        # 4. Aggregate: Calculate business metrics
        daily_metrics = transformed_df.groupBy("date", "product_category") \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                max("amount").alias("max_amount"),
                min("amount").alias("min_amount")
            )
        
        # 5. Load: Write results using partitioning
        # Detailed data
        transformed_df.write.mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(f"{output_path}/detailed")
        
        # Aggregated metrics
        daily_metrics.write.mode("append") \
            .partitionBy("date") \
            .parquet(f"{output_path}/metrics")
        
        # 6. Update checkpoint for incremental processing
        if checkpoint_path:
            max_date_processed = transformed_df.agg(max("date")).first()[0]
            checkpoint_data = [(max_date_processed, processing_date)]
            checkpoint_df = spark.createDataFrame(
                checkpoint_data, 
                ["last_processed_date", "processing_date"]
            )
            checkpoint_df.write.mode("overwrite").parquet(checkpoint_path)
        
        # 7. Collect and log processing statistics
        stats = [
            ("processing_date", processing_date),
            ("records_processed", transformed_df.count()),
            ("invalid_records", invalid_records.count()),
            ("latest_date_processed", str(max_date_processed)),
            ("processing_timestamp", datetime.datetime.now().isoformat())
        ]
        
        stats_df = spark.createDataFrame(stats, ["metric", "value"])
        stats_df.write.mode("append").parquet(f"{output_path}/stats")
        
        return stats_df
        
    except Exception as e:
        # Error handling
        print(f"Error in batch processing: {str(e)}")
        # Log the error
        error_df = spark.createDataFrame([
            (processing_date, str(e), datetime.datetime.now().isoformat())
        ], ["processing_date", "error", "timestamp"])
        error_df.write.mode("append").parquet(f"{output_path}/process_errors")
        raise e
    finally:
        # Clean up (optional)
        spark.stop()

# Example usage:
# batch_process(
#     input_path="/data/raw/sales",
#     output_path="/data/processed/sales",
#     processing_date="2023-01-20",
#     checkpoint_path="/data/checkpoints/sales"
# )
```

#### Knowledge Check

> **Question**: What are the key advantages of implementing the Dead Letter Queue pattern in batch processing?
> 
> **Answer**: The Dead Letter Queue pattern provides several advantages: 1) It allows the main processing flow to continue even when some records have errors, improving resilience; 2) It preserves invalid data for later analysis, troubleshooting, or reprocessing rather than simply discarding it; 3) It provides valuable feedback about data quality issues that can be addressed upstream; and 4) It helps maintain a complete audit trail of all data, including records that couldn't be processed normally.

## Chapter Summary

In this chapter, we've explored the fundamental concepts and techniques of data processing in Apache Spark. We covered the following key areas:

- **Spark Architecture**: We examined the components of Spark's distributed architecture, including the driver, executors, and cluster manager, understanding how they work together to process data at scale.

- **Spark Environment**: We learned how to configure and customize a Spark session for different workloads, understand various deployment models, and manage runtime parameters.

- **DataFrame Operations**: We explored creating, manipulating, and analyzing DataFrames, which are the primary abstraction for structured data processing in Spark.

- **Data Manipulation**: We covered a wide range of transformations, from basic operations to advanced techniques like aggregations, joins, and window functions.

- **Spark SQL**: We learned how to leverage SQL for data processing in Spark, creating temporary views, writing queries, and understanding query optimization.

- **Complex Data Types**: We worked with arrays, maps, structs, and nested schemas, which are essential for handling real-world, semi-structured data.

- **ETL Operations**: We implemented common Extract, Transform, Load patterns, focusing on data cleaning, validation, transformation, and loading techniques.

- **Batch Processing Patterns**: We examined patterns like incremental processing, windowed analysis, partitioning strategies, and error handling to build robust data pipelines.

These concepts and techniques form the foundation of data processing with Apache Spark. In the next chapter, we'll build on this knowledge to explore real-time data processing with Spark Structured Streaming.

## Review Questions

1. What is the primary difference between transformations and actions in Spark, and how does this relate to Spark's execution model?
   - A. Transformations modify data immediately, while actions create execution plans
   - B. Transformations create new RDDs/DataFrames, while actions trigger computation
   - C. Transformations run on the driver, while actions run on executors
   - D. Transformations work on DataFrames, while actions work on RDDs

2. Explain how partitioning affects Spark performance, and describe at least two different partitioning strategies you might use for different scenarios.

3. When implementing an ETL pipeline in Spark, what are some key considerations for handling data quality issues? Describe at least three techniques you could use.

4. Compare and contrast the full refresh and incremental update patterns for batch processing. Under what circumstances would you choose one over the other?

5. How can you optimize a join operation between a large DataFrame (1TB) and a small DataFrame (10MB) in Spark?

## Further Reading

- **Spark: The Definitive Guide** by Bill Chambers and Matei Zaharia: Comprehensive coverage of Spark fundamentals and advanced topics.

- **Learning Spark, 2nd Edition** by Jules Damji et al.: Updated guide covering modern Spark concepts with a focus on DataFrames and Spark SQL.

- **Apache Spark Documentation**: [https://spark.apache.org/docs/latest/](https://spark.apache.org/docs/latest/) - Official documentation with detailed API references and programming guides.

- **Databricks Blog**: [https://databricks.com/blog/category/engineering](https://databricks.com/blog/category/engineering) - Technical articles and best practices from the creators of Spark.
