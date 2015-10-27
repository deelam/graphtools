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
* void import(sourceData, graphUri)
* User needs to provide an input-data-specific Encoder that maps IORecord to GraphRecords.  The data import is done by the Importer.
  * souceData -> getNextRecord() -> IORecord -> encode() -> GraphRecord -> sort,merge,save() -> Graph
  * IORecord sourceData.getNextRecord()
  * List\<GraphRecord> encode(ioRecord, recordContext)
    * A single ioRecord can map to multiple GraphRecords.
    * This typically results in duplicate nodes, which are later sorted and merged before saving to the graph.
    * For efficiency, recordContext can hold a Map<StringId,Node> to avoid populating nodes with the same properties and speeding up node merging later.
    * The Encorder defines a set of RelationBuilders that declare nodes and edges to be created for GraphRecord.
    * A RelationBuilder defines functions that, when given an ioRecord, provide the id, type/label, and properties of nodes and edges.  To reduce the risk of inconsistent graph schemas and having to reconcile and merge later, these functions should be reused and shared where possible.

#### Graph URIs
* file://[host[:port]]/path
* hdfs://[host[:port]]/path
* titan://[host[:port]]/[hbase|cassandra]/tablename
* neo4j://[host[:port]]/path
* orientdb://[host[:port]]/path

#### GraphRecord
A dataset is modeled as individual, independent records to facilitate distributed processing.  Duplicate GraphRecords are merged in the final graph.
* IORecord is a generic interface for a data record in a source or output dataset
* GraphRecord represents a node and its incident edges
  * easily serializable, i.e., no dependence on third-party libraries
* Node: id, type, properties
* Edge: id, label, properties, srcNodeId, dstNodeId

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





