# Core Components

## GraphRecord
An input (or output) dataset is modeled as containing individual records to facilitate distributed processing.  Similarly, a GraphRecord is an individual record from a graph.
* DataSource.getNextRecord() provides a POJO data record from the input dataset.
* A GraphRecord instance represents a node and its incident edges.  It contains:
  * the node's id, type, and properties
  * and each edge's id, label, and properties. An edge's endpoints are stored as Strings representing the id of the source and destination nodes.  As a result a GraphRecord is very compact.
  * It is easily serializable, i.e., has no dependence on third-party classes. 

* *TODO*: Provide utilities to write a GraphRecord as a Hadoop Writable.

## Graph URIs
To specify a graph as a destination for importing or as source of data, a GraphUri is used.  The following URIs are supported (see GraphUriTest.java for example usage):
* "tinker:" - in-memory TinkerGraph (from Blueprints library)
* "tinker:./target/tGraph" - a TinkerGraph saved as a binary file in the ./target/tGraph relative directory 
* "tinker:./target/tGraphML?fileType=graphml" - a TinkerGraph saved in GraphML format in the ./target/tGraphML directory.  See TinkerGraph.FileType for other supported file types.
* "neo4j:./myNeoDir" - a Neo4jGraph stored in the ./myNeoDir directory
* "orientdb:[memory,plocal]:/tmp/myOrientDbGraph" - an OrientGraph in-memory or stored in the /tmp/myOrientDbGraph directory

*TODO*: The following may be supported
* "titan://[host[:port]]/hbase/tablename" - an HBase-backed TitanGraph 
* "titan://[host[:port]]/cassandra/tablename" - a Cassandra-backed TitanGraph 

To support other schemes, an IdGraphFactory must be registered using GraphUri.register(scheme, factory). 
