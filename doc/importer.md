# Importer

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
