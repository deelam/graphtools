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

## Problem characteristics
* There is no single graph schema to rule them all (for different datatypes, domains, and usecases).
* Interpretation of sourceData is not trivial -- requires domain knowledge.  Hence, we enable those experts to map the sourceData into a graph representation, which is more easily interpretable.
* Even when given a graph representation of the data, it needs to be translated into a graph schema that is at least compatible (if not consistent) with other graphs.
* It may not be efficient or reasonable to merge graphs, which duplicate data from the original graphs.  So, we want to minimize copying data when possible.

## GraphTools

The tools are categorized into the following levels:
* [Level 0 Tools](doc/level0tools.md): 
 * create and delete a graph database
 * data importer to build graphs
 * exporter to different graph file formats
* [Level 1 Tools](doc/level1tools.md)
 * (read-only transform) graph translator to filter node or edge types, collapse graph paths, or transform a graph into another graph; 
 * (append-only) graph enricher to add data to a graph 
 * (read-write) graph modifier to write and delete graph data
* [Level 2 Tools](doc/level2tools.md)
 * merge
 * disambiguate
 * superimpose




