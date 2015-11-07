# graphtools
Toolsuite of small, reusable tools to operate on graphs

## Big Picture
These tools are meant to 
* import data into graph representation
* export graphs into other formats
* facilitate use of "Big Data" tools (e.g., Hadoop, Spark, Titan) 
* transform one graph representation into another graph with a different schema
* reduce boilerplate infrastructure code to expediate development

These tools use
* Tinkerpop's Blueprints interfce to be independent of graph implementations
* Currently focus on OLAP
 
By combining these tools, we can scalably import data in various formats and store it in graph databases; "merge" the graphs so they have a consistent schema; and analyze, extract, and transform the merged graph into desired output.

<pre>
+-------------+      +-----------------+
| sourceData1 +----->+     graph1      +---+
+-------------+      | (using schema1) |   |    +-----------------+
                     +-----------------+   +--->+   mergedGraph   |
                                           |    | (using schema3) |
+-------------+      +-----------------+   |    +--------+--------+
| sourceData2 +----->+     graph2      +---+             |
+-------------+      | (using schema2) |                 |
                     +-----------------+                 v
                                                   +-----+------+
                                                   | outputData |
                                                   +------------+
</pre>

Problem characteristics
* There is no single graph schema to rule them all (for different datatypes, domains, and usecases).
* Interpretation of sourceData is not trivial -- requires domain knowledge.  Hence, we enable those experts to map the sourceData into a graph representation, which is more easily interpretable.
* Even when given a graph representation of the data, it needs to be translated into a graph schema that is at least compatible (if not consistent) with other graphs.
* It may not be efficient or reasonable to merge graphs, which duplicate the original graph data.

## Level 0 Tools

### Importer
* Importing data into a graph is decomposed into 3 modular components:
  1. a domain-specific Parser: parses a record from the sourceData into a POJO
  2. a domain-specific Encoder: maps a POJO to one or more GraphRecords
    * An Encorder defines a set of EntityRelations that declare nodes and edges to be created and stored in a GraphRecord.
    * An EntityRelation effectively define NodeFiller and EdgeFiller functions that, when given an POJO, provide the id, node type or edge label, and properties of nodes and edges.  To reduce the risk of inconsistent graph schemas and having to reconcile and merge later, these functions should be reused and shared where possible.  As an example, see the CompanyContactsEncorder class in the src/test directory.
  3. a Populator: add GraphRecords to the specified graph, potentially performing buffering, sorting, and merging duplicates to improve performance. 
* The user needs to provide domain-specific Encoder and Parser classes.  Existing implementations provide the gluecode and pipelining to accomplish the data import.  To parse a CSV-formatted file for example:
  * First, the CsvBeanSourceDataFactory<POJO> uses the provided CsvParser to interpret the CSV file can create POJO instances.  Specifically, it creates a CsvFileToBeanSourceData instance that, when getNextRecord() is called, uses information from the CsvParser to create a POJO for each CSV record (typically a line in the file).
    * CsvBeanSourceDataFactory creates an appropriate SourceData instance depending on how the input data is stored, such as from a File or Readable class.  
  * The DefaultImporter gets a POJO by calling SourceData.getNextRecord() and uses the provided Encoder to produce a Collection<GraphRecord> that represents the POJO in graph form.  The GraphRecord section below describes how it is modeled.
  * The DefaultPopulator then uses the GraphRecords to create a graph.
  * In summary, the dataflow is: souceData -> getNextRecord() -> POJO -> encode() -> GraphRecord -> populate() -> Graph
  * Control-flow is managed by the DefaultImporter, which processes one CSV record at a time, calling the Parser, Encoder, and Populator for each CSV record.  
    * Note: a single CSV record can map to multiple GraphRecords.  This typically results in duplicate nodes, which are later sorted and merged by the Populator before saving to the graph.  The Populator has the option of buffering the GraphRecords or immediately writing to the graph.
    * For efficiency, DefaultImporter holds a Map<String,GraphRecord> to avoid populating nodes with the same properties and speeding up node merging later.
* *TODO*: Analogous SourceDataFactory, Importer, and Populator should be added for other types and formats of input data.

* The ImporterManager can maintain a registry of different importers.  It has several entry-point methods to facilitate data import into a graph.

#### GraphRecord
An input (or output) dataset is modeled as containing individual records to facilitate distributed processing.  Similarly, a GraphRecord is an individual record from a graph.
* DataSource.getNextRecord() provides a POJO data record from the input dataset.
* A GraphRecord instance represents a node and its incident edges.  It contains:
  * the node's id, type, and properties
  * and each edge's id, label, and properties. An edge's endpoints are stored as Strings representing the id of the source and destination nodes.  As a result a GraphRecord is very compact.
  * It is easily serializable, i.e., has no dependence on third-party classes. 

* *TODO*: Provide utilities to write a GraphRecord as a Hadoop Writable.

#### Graph URIs
To specify a graph as a destination for importing or as source of data, a GraphUri is used.  The following URIs are supported (see GraphUriTest.java for example usage):
* "tinker:///" - in-memory TinkerGraph (from Blueprints library)
* "tinker:///./target/tGraph" - a TinkerGraph saved as a binary file in the ./target/tGraph relative directory 
* "tinker:///./target/tGraphML?fileType=graphml" - a TinkerGraph saved in GraphML format in the ./target/tGraphML directory.  See TinkerGraph.FileType for other supported file types.

*TODO*: The following will soon be supported
* "neo4j://[host[:port]]/./myNeoDir" - a Neo4jGraph stored in the ./myNeoDir directory
* "orientdb://[host[:port]]/tmp/myOrientDbGraph" - an OrientDbGraph stored in the /tmp/myOrientDbGraph directory
* "titan://[host[:port]]/hbase/tablename" - an HBase-backed TitanGraph 
* "titan://[host[:port]]/cassandra/tablename" - a Cassandra-backed TitanGraph 

The "tinker" URI scheme is supported by default.  To support other schemes, an IdGraphFactory must be registered using GraphUri.register(scheme, factory). 

### Exporter
Leverages existing exporting utilities provided by Bluepints.
* IORecord export(GraphRecord)

### Inspector (Graph Schema)
* extractSchema(graph)
* extractSchemaWithSamples(graph)
* extractSchemaWithStats(graph)
* diffSchemas(graph1,graph2)
* validateAgainstSchema(schema, graph)

## Level 1 Tools
### Translator
Used to filter node or edge types, collapse graph paths, or transform a graph into another graph.
* List\<GraphRecord> translate(GraphRecord)

### Enricher (append-only)
* void enrich(graph)

### Modifier (write and delete) (Level 1)
* void modify(graph)

### OLTP
Once in a graph database, use other tools to random access.

## Level 2 Tools

### Merger
* mergeBasedOnId
* mergeBasedOnEquivalence
* mergeBasedOnSimilarity

#### Equivalence/Similarity measures

### Superimposer
Imposes a new graph layer that connects multiple isolated child graphs via equivalence links without merging child graphs.





