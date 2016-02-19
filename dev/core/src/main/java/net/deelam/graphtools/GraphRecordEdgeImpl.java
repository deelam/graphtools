package net.deelam.graphtools;

import lombok.Getter;
import lombok.NoArgsConstructor;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

/**
 * This class is created only for ingestion.
 * 
 * @author deelam
 */
@NoArgsConstructor
public class GraphRecordEdgeImpl extends GraphRecordElementImpl implements GraphRecordEdge {

  private static final long serialVersionUID = 201509030420L;

  @Getter
  protected String label;

  @Getter
  protected String outVertexStringId;
  @Getter
  protected String inVertexStringId;

  public GraphRecordEdgeImpl emptyCopy() {
    return new GraphRecordEdgeImpl(getStringId(), label, outVertexStringId, inVertexStringId);
  }

  public GraphRecordEdgeImpl(String id, String label, String outVertex, String inVertex) {
    super(id);
    this.label = label;
    outVertexStringId = outVertex;
    inVertexStringId = inVertex;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  //private static int counter=0;

  ///================

  @Override
  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    if (direction == Direction.OUT)
      return new GraphRecordImpl(outVertexStringId);

    if (direction == Direction.IN)
      return new GraphRecordImpl(inVertexStringId);

    throw new UnsupportedOperationException();
  }

  /// ==========

//  private static final String OUT_VERTEX_LONG_ID = Direction.OUT.toString() + LONG_ID_PROPKEY;
//  private static final String IN_VERTEX_LONG_ID = Direction.IN.toString() + LONG_ID_PROPKEY;

  public void setInNodeId(long id) {
    setNodeId(Direction.IN, id);
  }

  public void setOutNodeId(long id) {
    setNodeId(Direction.OUT, id);
  }

  public Long getInNodeId() {
    return getNodeId(Direction.IN);
  }

  public Long getOutNodeId() {
    return getNodeId(Direction.OUT);
  }

  public void setNodeId(Direction dir, long id) {
    setProperty(dir.toString() + LONG_ID_PROPKEY, id);
  }

  public Long getNodeId(Direction dir) {
    return getProperty(dir.toString() + LONG_ID_PROPKEY);
  }

  public String toString() {
    return "EdgeWritable[" + super.toString() + ",outStrId=" + outVertexStringId + " inStrId="
        + inVertexStringId + "]";
  }
}
