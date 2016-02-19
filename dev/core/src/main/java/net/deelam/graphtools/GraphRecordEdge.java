package net.deelam.graphtools;

import java.io.Serializable;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;



public interface GraphRecordEdge extends Edge, GraphRecordElement, Serializable{

  GraphRecordEdgeImpl emptyCopy();
  
  String getInVertexStringId();
  
  String getOutVertexStringId();

  void setInNodeId(long id);

  void setOutNodeId(long id);

  Long getOutNodeId();

  void setNodeId(Direction dir, long id);

}
