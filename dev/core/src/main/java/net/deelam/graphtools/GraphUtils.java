package net.deelam.graphtools;

import static com.google.common.base.Preconditions.*;

import java.util.Date;

import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.wrappers.WrapperGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * Operations on Blueprints graphs
 * @author deelam
 */
@Slf4j
public final class GraphUtils {

  //// Read-only operations
  /*
   * If looking up elements by properties, the graph should be indexed.
   */

  //	public static Edge getEdge(final KeyIndexableGraph graph, final String key, final Object value){
  //		return Iterables.getOnlyElement(graph.getEdges(key, value));
  //	}
  //
  //	public static Vertex getNode(final KeyIndexableGraph graph, final String key, final Object value){
  //		return Iterables.getOnlyElement(graph.getVertices(key, value));
  //	}

  public static boolean hasSameEndpoints(Edge e, Edge e2) {
    Vertex outV = e2.getVertex(Direction.OUT);
    Vertex inV = e2.getVertex(Direction.IN);
    return GraphUtils.hasEndpoints(e2, outV, inV);
  }

  public static boolean hasEndpoints(Edge e, Vertex outV, Vertex inV) {
    Vertex outV1 = e.getVertex(Direction.OUT);
    Vertex inV1 = e.getVertex(Direction.IN);
    boolean equalsEndpoints = true;
    if (!outV1.equals(outV)) {
      log.error("outVertex not equal: " + outV1 + "!=" + outV);
      equalsEndpoints = false;
    }
    if (!inV1.equals(inV)) {
      log.error("inVertex not equal: " + inV1 + "!=" + inV);
      equalsEndpoints = false;
    }
    return equalsEndpoints;
  };

  public static long getNodeCount(Graph graph) {
    return Iterables.size(graph.getVertices());
  }

  public static long getEdgeCount(Graph graph) {
    return Iterables.size(graph.getEdges());
  }

  public static String count(Graph graph) {
    StringBuilder sb = new StringBuilder(graph.toString());
    sb.append(": ");
    sb.append(getNodeCount(graph)).append(" nodes, ");
    sb.append(getEdgeCount(graph)).append(" edges");
    return (sb.toString());
  }

  public static String toString(Graph graph) {
    return toString(graph, -1);
  }

  public static String toString(Graph graph, int numSamples) {
    return toString(graph, numSamples, (String[]) null);
  }

  public static String toString(Graph graph, int numSamples, String... propsToPrint) {
    StringBuilder sb = new StringBuilder(graph.toString());
    sb.append("\n Nodes:\n");
    int nodeCount = 0;
    for (Vertex n : graph.getVertices()) {
      ++nodeCount;
      // Note that IdGraph.ID is removed from the IdElement.propertyKeys() list
      if (numSamples < 0 || nodeCount < numSamples) {
        sb.append("  ").append(n.getId()).append(": ");
        sb.append(n.getPropertyKeys());
        sb.append("\n    ").append(toString(n, "\n    ", propsToPrint)).append("\n");
      }
    }
    sb.append(" Edges:\n");
    int edgeCount = 0;
    for (Edge e : graph.getEdges()) {
      ++edgeCount;
      if (numSamples < 0 || nodeCount < numSamples) {
        sb.append("  ").append(e.getLabel()).append(" ").append(e.getId()).append(" (");
        sb.append(e.getVertex(Direction.OUT)).append("->").append(e.getVertex(Direction.IN));
        sb.append("): ");
        sb.append(e.getPropertyKeys());
        sb.append("\n    ").append(toString(e, "\n    ", propsToPrint)).append("\n");
      }
    }
    sb.append("(").append(nodeCount).append(" nodes, ").append(edgeCount).append(" edges)");
    return (sb.toString());
  }

  public static String toString(Element n, String delim, String... propsToPrint) {
    StringBuilder sb = new StringBuilder();
    if (propsToPrint != null) {
      if (propsToPrint.length == 0) {
        propsToPrint = (String[]) n.getPropertyKeys().toArray(propsToPrint);
      }
      boolean first = true;
      for (String propKey : propsToPrint) {
        if (n.getProperty(propKey) != null) {
          if (first) {
            first = false;
          } else {
            sb.append(delim);
          }
          sb.append(propKey).append("=").append(n.getProperty(propKey));
        }
      }
    }
    return sb.toString();
  }

  public static String toString(Element n, String delim, boolean printId) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    if (printId) {
      sb.append("id=").append(n.getId());
      first = false;
    }
    for (String propKey : n.getPropertyKeys()) {
      if (n.getProperty(propKey) != null) {
        if (first) {
          first = false;
        } else {
          sb.append(delim);
        }
        sb.append(propKey).append("=").append(n.getProperty(propKey));
      }
    }
    return sb.toString();
  }

  public static String toString(Element n) {
    return "\n\t" + ((n instanceof Edge) ? ((Edge) n).getLabel() + "\n\t" : "")
        + toString(n, "\n\t", true);
  }

  public static String toString(Iterable<? extends Element> elems) {
    StringBuilder sb = new StringBuilder();
    for (Element n : elems) {
      sb.append("\n\t").append(toString(n, "\n\t", true));
    }
    return sb.toString();
  }


  // returns whether currGraph is nested at some depth within g
  public static boolean isWrappedWithin(Graph g, Graph currGraph) {
    while (g instanceof WrapperGraph) {
      g = ((WrapperGraph<?>) g).getBaseGraph();
      if (g == currGraph)
        return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Graph> T unwrapToGraphType(Class<T> clazz, Graph g) {
    if (clazz.isInstance(g))
      return (T) g;

    while (g instanceof WrapperGraph) {
      g = ((WrapperGraph<?>) g).getBaseGraph();
      if (clazz.isInstance(g))
        return (T) g;
    }
    return null;
  }


  /* 
   * WRITE operations
   */

  public static void clearGraph(Graph graph) {
    checkNotNull(graph);
    for (Edge e : graph.getEdges()) {
      graph.removeEdge(e);
    }

    for (Vertex v : graph.getVertices()) {
      graph.removeVertex(v);
    }
  }

  public static void clearGraph(TransactionalGraph graph) {
    checkNotNull(graph);
    int tx = GraphTransaction.begin(graph);
    try {
      for (Edge e : graph.getEdges()) {
        graph.removeEdge(e);
      }

      for (Vertex v : graph.getVertices()) {
        graph.removeVertex(v);
      }
      GraphTransaction.commit(tx);
    } catch (RuntimeException re) {
      GraphTransaction.rollback(tx);
      throw re;
    }
  }

  ///

  public static final String METADATA_VERTEXID = "_GRAPH_METADATA_";
  private static final String TIMESTAMP_PROP = "_TIMESTAMP_";
  private static final String GRAPHURI_PROP = "_GRAPHURI_";

  //  private static final String VERTEXTYPES_PROP = "_VERTEXTYPES_";
  //  private static final String EDGELABELS_PROP = "_EDGELABELS_";

  public static Vertex getMetaDataNode(IdGraph<?> graph) {
    Vertex mdV = graph.getVertex(METADATA_VERTEXID);
    return mdV;
  }

  public static Vertex setMetaData(IdGraph<?> graph, String propKey, Object propValue) {
    Vertex mdV = getMetaDataNode(graph);
    mdV.setProperty(propKey, propValue);
    return mdV;
  }

  public static <T> T getMetaData(IdGraph<?> graph, String propKey) {
    Vertex mdV = getMetaDataNode(graph);
    return mdV.getProperty(propKey);
  }

  public static void addMetaDataNode(GraphUri gUri, IdGraph<?> graph) {
    Vertex mdV = getMetaDataNode(graph);
    if (mdV == null) {
      mdV = graph.addVertex(METADATA_VERTEXID);
      mdV.setProperty(TIMESTAMP_PROP, new Date().toString());
      mdV.setProperty(GRAPHURI_PROP, gUri.asString());
      //      mdV.setProperty(VERTEXTYPES_PROP, gUri.getVertexTypes());
      //      mdV.setProperty(EDGELABELS_PROP, gUri.getEdgeLabels());
    }
  }

  ///

  public static final Direction[] BOTHDIR = {Direction.OUT, Direction.IN};

  /**
   * Assumes both nodes are in the same graph.
   * Removes original edges and node.
   * @param propMerger to merge node and edge properties
   */
  public static void mergeNodesAndEdges(Vertex origV, Vertex targetV, Graph graph, PropertyMerger propMerger) {
    propMerger.mergeProperties(origV, targetV);
    moveEdges(origV, targetV, graph);
    graph.removeVertex(origV);
  }

  /**
   * 
   */
  public static void moveEdges(Vertex origV, Vertex targetV, Graph graph) {
    for (Direction dir : BOTHDIR)
      for (Edge edge : origV.getEdges(dir)) {
        Vertex neighbor = edge.getVertex(dir.opposite());
        // copy edge in memory before removing from graph
        GraphRecordEdge inMemEdge = new GraphRecordEdge((String) edge.getId(), edge.getLabel(),
            (String) targetV.getId(), (String) neighbor.getId());
        ElementHelper.copyProperties(edge, inMemEdge);
        graph.removeEdge(edge);

        Edge eCopy;
        if (dir == Direction.OUT) {
          eCopy = graph.addEdge(inMemEdge.getId(), targetV, neighbor, inMemEdge.getLabel());
        } else {
          eCopy = graph.addEdge(inMemEdge.getId(), neighbor, targetV, inMemEdge.getLabel());
        }
        ElementHelper.copyProperties(inMemEdge, eCopy);
      }
  }

}
