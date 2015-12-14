# Level 2 Tools


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

