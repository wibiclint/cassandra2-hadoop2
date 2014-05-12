cassandra2-hadoop2
==================

Hadoop `InputFormat` for Cassandra.  Works with Hadoop-0.21+ and Cassandra 2.x.

Features beyond those supported in the standard InputFormat (at least when I started working on
this!):

* Works with Hadoop 2
* Supports combining results from multiple queries over multiple tables (this works as long as the
  different tables have the same primary key)
* Supports grouping together Rows based on their partition key and an optional subset of clustering
  columns

To use:

- Download and install this Maven Cassandra plugin: https://github.com/wibiclint/simple-cassandra-maven-plugin
- Build and install into your local Maven repo: `mvn clean install`
- Include as a dependency in the POM for your project:

        <dependency>
            <groupId>com.github.cassandra2-hadoop2</groupId>
            <artifactId>cassandra2-hadoop2</artifactId>
            <version>0.1-SNAPSHOT</version>
        </dependency>



Based originally on work from
[michaelsembwever](https://github.com/michaelsembwever/cassandra-hadoop), which stems from Dave
Brosius's work in CASSANDRA-5201.
