package net.deelam.graphtools.importer;

import java.util.Collection;
import java.util.Map.Entry;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphRecordEdge;
import net.deelam.graphtools.GraphRecordMerger;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

@RequiredArgsConstructor
//@Slf4j
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

  public Vertex importVertex(IdGraph<?> graph, GraphRecord gr) {
    String id = gr.getStringId();
    Vertex newV = graph.getVertex(id);
    if (newV == null) {
      newV = graph.addVertex(id);
    }
    copyProperties(gr, newV);
    return newV;
  }

  public Edge importEdge(IdGraph<?> graph, GraphRecordEdge grE, Direction direction,
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

  @Getter
  final GraphRecordMerger graphRecordMerger;
  
  public void copyProperties(Element fromE, Element toE) {
    graphRecordMerger.mergeProperties(fromE, toE);
  }
}
