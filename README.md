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
* It may not be efficient or reasonable to merge graphs, which duplicate data from the original graphs.  So, we want to minimize copying data when possible.

## Level 0 Tools

### Importer
Importing data into a graph is decomposed into 3 modular components: a domain-specific Parser, a domain-specific Encoder, and a Populator.  The user needs to provide domain-specific Encoder and Parser classes.  Existing implementations provide the gluecode and pipelining to accomplish the data import.  


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
* "tinker:" - in-memory TinkerGraph (from Blueprints library)
* "tinker:./target/tGraph" - a TinkerGraph saved as a binary file in the ./target/tGraph relative directory 
* "tinker:./target/tGraphML?fileType=graphml" - a TinkerGraph saved in GraphML format in the ./target/tGraphML directory.  See TinkerGraph.FileType for other supported file types.
* "neo4j:./myNeoDir" - a Neo4jGraph stored in the ./myNeoDir directory
* "orientdb:[memory,plocal]:/tmp/myOrientDbGraph" - an OrientGraph in-memory or stored in the /tmp/myOrientDbGraph directory

*TODO*: The following may be supported
* "titan://[host[:port]]/hbase/tablename" - an HBase-backed TitanGraph 
* "titan://[host[:port]]/cassandra/tablename" - a Cassandra-backed TitanGraph 

To support other schemes, an IdGraphFactory must be registered using GraphUri.register(scheme, factory). 

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
Goal: Enrich/Consolidate data by linking equivalent entities.
  1. Bin nodes (representing entities) based on hash functions that map node properties into normalized strings or numbers (their hash), where node properties with the same hash are candidates to be assessed for equivalency.  Two nodes with many properties that map to the same hash are more likely to be equivalent.  Different hash functions should be combined to check for equivalence.  For example, name and phone hash functions can be used to map node "John Smith" with phone number "1-234-567-8900" to "JS" and "5678900", respectively.  Another node with name "J. Smith" and number "5678900" would also be mapped to the same hashes, and so these two nodes would be candidates for equivalence assessment.  A bin contains all entities with the same hash, and there a different bin types (for example, name and phone number types).
  2. For each bin type, for each candidate set, add each candidate as an entity node in an Equivalency Graph (EG).  Each pair of nodes is connected by an edge with a score property representing entity similarity.  The result is a complete graph (all nodes connected to every other node), with some edges having higher scores than others.  There may be multiple scores, one for each bin type, on the edge.
  3. For each complete graph, create a "Meta-entity node" and connect it to the subset of nodes with highest edge scores.  These nodes are the most likely to be equivalent entities.  The other nodes are less likely to be equivalent, and thus should not be connected to the Meta-entity node.
  4. At this point, the connected

Each data type (e.g., person name or phone number) has an associated hash function, bin type, equivalency-score edge property with associated scoring function for the data type, and . 


### Superimposer
Imposes a new graph layer that connects multiple isolated child graphs via equivalence links without merging child graphs.





