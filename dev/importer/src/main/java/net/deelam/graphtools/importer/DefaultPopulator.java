package net.deelam.graphtools.importer;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Date;
import java.util.Map.Entry;

import net.deelam.graphtools.GraphRecord;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

@RequiredArgsConstructor
@Slf4j
public class DefaultPopulator implements Populator {

  @Getter
  private final String importerName;

  private static final String IMPORTER_KEY = "_ingester";

  private void markRecords(Collection<GraphRecord> gRecords) {
    for (GraphRecord rec : gRecords) {
      rec.setProperty(IMPORTER_KEY, importerName);
      for (Entry<String, Edge> e : rec.getOutEdges().entrySet()) {
        e.getValue().setProperty(IMPORTER_KEY, importerName);
      }
      for (Entry<String, Edge> e : rec.getInEdges().entrySet()) {
        e.getValue().setProperty(IMPORTER_KEY, importerName);
      }
    }
  }

  @Override
  public void populateGraph(IdGraph<?> graph, Collection<GraphRecord> gRecords) {
    if (importerName != null) {
      markRecords(gRecords);
    }
    // TODO: sort and merge

    // add to graph
    for (GraphRecord v : gRecords) {
      Vertex newV = importVertex(graph, v);
      copyProperties(v, newV);
      importEdges(graph, Direction.OUT, newV, v);
      importEdges(graph, Direction.IN, newV, v);
    }
  }


  private void importEdges(Graph graph, Direction direction, Vertex newV, Vertex vertex) {
    for (Edge e : vertex.getEdges(direction)) {
      Vertex oppV = e.getVertex(direction.opposite());
      Vertex newOpp = importVertex(graph, oppV);
      importEdge(graph, e, direction, newV, newOpp);
    }
  }

  public static Vertex importVertex(Graph graph, Vertex v) {
    return importVertex(graph, v, v.getId());
  }

  public static Vertex importVertex(Graph graph, Vertex v, Object id) {
    Vertex newV = graph.getVertex(id);
    if (newV == null) {
      newV = graph.addVertex(id);
      copyProperties(v, newV);
      String origId = v.getProperty(IdGraph.ID);
      if (origId == null) {
        origId = v.getProperty("__origId");
      }
      if (origId == null) {
        origId = (String) v.getId();
      }
      newV.setProperty("__origId", origId);
    }
    return newV;
  }

  public static Edge importEdge(Graph graph, Edge e, Direction direction, Vertex v1inGraph,
      Vertex v2inGraph) {
    final String edgeId = e.getId().toString();
    Edge newEdge = graph.getEdge(edgeId);
    if (newEdge == null) {
      if (direction == Direction.OUT)
        newEdge = graph.addEdge(edgeId, v1inGraph, v2inGraph, e.getLabel());
      else
        newEdge = graph.addEdge(edgeId, v2inGraph, v1inGraph, e.getLabel());
      copyProperties(e, newEdge);
    } else {
      checkArgument(newEdge.getVertex(direction).equals(v1inGraph), "Expecting " + v1inGraph
          + " but got " + newEdge.getVertex(direction) + " for edge " + edgeId);
      checkArgument(newEdge.getVertex(direction.opposite()).equals(v2inGraph));
    }
    return newEdge;
  }

  static final String SET_SUFFIX = "_.SET_";

  public static void copyProperties(Element fromE, Element toE) {
    for (String key : fromE.getPropertyKeys()) {
      if (key.equals(IdGraph.ID))
        continue;
      if (key.endsWith(SET_SUFFIX)) { // ignore
        switch (key) {
          default:
            log.warn("Ignoring property: " + fromE + " " + key + "=" + fromE.getProperty(key));
            continue;
        }
      }

      //					System.out.println("  setting property=" + pkey);
      Object toValue = toE.getProperty(key);
      Object fromValue = fromE.getProperty(key);
      if (fromValue == null) {
      } else if (toValue == null) {
//        if (fromValue instanceof Date) {
//          log.warn("Converting from Date to String: " + fromValue+"  and setting new property "+key+"_millis");
//          toE.setProperty(key+"_millis", ((Date)fromValue).getTime());
//          fromValue = fromValue.toString();
//        }
        toE.setProperty(key, fromValue);
      } else if (toValue.equals(fromValue)) { // nothing to do
      } else { // toValue and fromValue are not null and not equal

        //				// Try https://github.com/thinkaurelius/titan/wiki/Datatype-and-Attribute-Serializer-Configuration
        //				// check special SET property
        String sPropertyKey = key + SET_SUFFIX;
        //				Set<Object> valueSet=toE.getProperty(sPropertyKey);
        //				if(valueSet == null){
        //					valueSet=new HashSet<>();
        //					toE.setProperty(sPropertyKey, valueSet);
        //					valueSet.add(fromValue);
        //				}
        //				valueSet.add(toValue);
        //				log.warn("Ignoring property due to multiple values: "+key + " valueSet={}", valueSet);
        // TODO: 0: all values are in the sPropertyKey as a String; need to make it accessible via key
        log.warn("Appending property values for '{}' origValue={} newValue={}", key, toValue,
            fromValue);
        toE.setProperty(sPropertyKey, toValue.toString() + ";" + fromValue.toString());
      }
    }
  }
}
