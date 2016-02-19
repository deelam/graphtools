package net.deelam.graphtools;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;



public interface GraphRecord extends Vertex, GraphRecordElement, Serializable {
  
  public static abstract class Factory {
    public abstract GraphRecord create(String id);
    public abstract GraphRecord create(String id, String nodeType);
    public abstract GraphRecordEdge createEdge(String id, String label, String outVertex, String inVertex);
    
    public GraphRecordEdge createEdge(String id, String label, Vertex outVertex, Vertex inVertex) {
      return createEdge(id, label, (String) outVertex.getId(), (String) inVertex.getId());
    }

    public GraphRecordEdge createEdge(String label, Vertex outVertex, Vertex inVertex) {
      return createEdge(newEdgeId(), label, (String) outVertex.getId(), (String) inVertex.getId());
    }

    protected String newEdgeId() {
      //return "edge" + counter++;
      return UUID.randomUUID().toString();
    }
  }

  String getType();

  Map<String, Edge> getInEdges();

  GraphRecordEdge getInEdge(String edgeId);

  Map<String, Edge> getOutEdges();

  GraphRecordEdge getOutEdge(String edgeId);

  void addEdge(GraphRecordEdge edge);

  void setLongId(long id);

  Long getLongId();

}
