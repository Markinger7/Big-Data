# Big-Data
A repo containing notes and code related to Big Data

## What is Big Data?
- volume (amount of Data)
- variety (different formats)
- velocity (in real time)
- validity (need for preprocessing)
- value (contains a lot of value )

## Data Lake vs. Datawarehouse
- Data Warehouse (ELT)
    - a fixed system, that transforms structured data in diffenert ways defined by rules
    - is a place for structured data (figures etc.)
    - 'on write' fix, after a predefined schema
- Data Lake
    - is a place for all of the raw data (even pictures, videos, audio)
    - 'on read' flexible, uses a schema defined by the demand (ad hoc

## Advantage distributed system for BIG DATA
- The cost of a distributed system is linear, whereas the cost of a local system is exponential
- distributed systems are easy to scale
- they can tolerate failure, e.g. when one machine breaks

## Hadoop Distributed File System (HDFS)
- a big data storage system that splits data into chunks and stores the chunks across a cluster of computers
- can tolerate failure, since it duplicates files across the machines
    - each block has 128mb and gets duplicated three times
- uses MapReduce
    - this is a method to distribute tasks over a distributed network
    - contains a Job tracker(NameNode) and multiple Task trackers (Data Node)

## Spark
- published in  2013 from UC Berkley
- is like a flexible Version of MapReduce
- can use files from different formats (Cassandra, AWS S3, HDFS and more)
- Spark contains libraries for data analysis, machine learning, graph analysis, and streaming live data

### Spark vs. MapReduce
- to use MapReduce, files need to be saved as HDFS
- Spark is faster than MapReduce (100x), because it saves to the ram and not to the hard disk

### Spark RDDs
- relies on the Ideas of the Resilient Distributed Dataset
    1. Distributed collection of data
    2. failure tolerant
    3. Parallel operations
    4. Ability to use many data sources
- RDD are 'lazy evaluated ' → code gets only activated as far as it is needed
- They hold their values in tuples, so called 'key-value-pairs'

### Byond Spark for stroing and processing Big Data
- Spark isn't a data storage system, and there are a number of tools besides Spark that can be used to process and analyze large datasets.
- Sometimes it makes sense to use the power and simplicity of SQL on big data. For these cases, a new class of databases, know as NoSQL and NewSQL, have been developed.
- There are also newer database storage systems like HBase or Cassandra. There are also distributed SQL engines like Impala and Presto. Many of these technologies use query syntax
