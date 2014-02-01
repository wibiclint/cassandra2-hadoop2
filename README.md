cassandra2-hadoop2
==================

Provides Hadoop-0.21+ integration into Cassandra 2.0.0.

Based on [work from michaelsembwever](https://github.com/michaelsembwever/cassandra-hadoop), which
stems from Dave Brosius work in CASSANDRA-5201.

Does not include any of the code for pig.

To use:
- Build and install into your local Maven repo: `mvn clean install`
- Include as a dependency in the POM for your project:

        <dependency>
            <groupId>com.github.cassandra2-hadoop2</groupId>
            <artifactId>cassandra2-hadoop2</artifactId>
            <version>0.1-SNAPSHOT</version>
        </dependency>

Enjoy!

