# Level 0 Tools

## Importer
Importing data into a graph is decomposed into 3 modular components: a domain-specific Parser, a domain-specific Encoder, and a Populator.  The user needs to provide domain-specific Encoder and Parser classes.  Existing implementations provide the gluecode and pipelining to accomplish the data import.  See [Importer](doc/importer.md).

## Exporter
Leverages existing exporting utilities provided by Bluepints.
* IORecord export(GraphRecord)

## Inspector (Graph Schema)
* extractSchema(graph)
* extractSchemaWithSamples(graph)
* extractSchemaWithStats(graph)
* diffSchemas(graph1,graph2)
* validateAgainstSchema(schema, graph)