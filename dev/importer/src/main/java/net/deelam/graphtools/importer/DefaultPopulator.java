package net.deelam.graphtools.importer;

import java.util.Collection;
import java.util.Map.Entry;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphRecordEdge;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
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

    // add to graph
    for (GraphRecord gr : gRecords) {
      Vertex newV = importVertex(graph, gr);
      importEdges(graph, Direction.OUT, newV, gr);
      importEdges(graph, Direction.IN, newV, gr);
    }
  }


  private void importEdges(IdGraph<?> graph, Direction direction, Vertex newV, GraphRecord gr) {
    for (Edge e : gr.getEdges(direction)) {
      GraphRecord oppV = (GraphRecord) e.getVertex(direction.opposite());
      Vertex newOppV = importVertex(graph, oppV);
      importEdge(graph, (GraphRecordEdge) e, direction, newV, newOppV);
    }
  }

  public static Vertex importVertex(IdGraph<?> graph, GraphRecord gr) {
    String id = gr.getStringId();
    Vertex newV = graph.getVertex(id);
    if (newV == null) {
      newV = graph.addVertex(id);
    }
    copyProperties(gr, newV);
    return newV;
  }

  public static Edge importEdge(IdGraph<?> graph, GraphRecordEdge grE, Direction direction,
      Vertex v1inGraph, Vertex v2inGraph) {
    String edgeId = grE.getStringId();
    Edge newEdge = graph.getEdge(edgeId);
    if (newEdge == null) {
      if (direction == Direction.OUT)
        newEdge = graph.addEdge(edgeId, v1inGraph, v2inGraph, grE.getLabel());
      else
        newEdge = graph.addEdge(edgeId, v2inGraph, v1inGraph, grE.getLabel());
    } else {
      if (!newEdge.getLabel().equals(grE.getLabel()))
        throw new IllegalArgumentException("Expecting " + grE.getLabel() + " but got "
            + newEdge.getLabel());
      if (!newEdge.getVertex(direction).equals(v1inGraph))
        throw new IllegalArgumentException("Expecting " + v1inGraph + " but got "
            + newEdge.getVertex(direction) + " for edge " + edgeId);
      if (!newEdge.getVertex(direction.opposite()).equals(v2inGraph))
        throw new IllegalArgumentException("Expecting " + v2inGraph + " but got "
            + newEdge.getVertex(direction.opposite()) + " for edge " + edgeId);
    }
    copyProperties(grE, newEdge);
    return newEdge;
  }

  static final String SET_SUFFIX = DefaultGraphRecordMerger.SET_SUFFIX;

  public static void copyProperties(Element fromE, Element toE) {
    DefaultGraphRecordMerger.mergeProperties(fromE, toE);
    /*
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
    }*/
  }
}
