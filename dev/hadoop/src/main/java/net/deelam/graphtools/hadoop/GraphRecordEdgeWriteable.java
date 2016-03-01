package net.deelam.graphtools.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.NoArgsConstructor;
import net.deelam.graphtools.GraphRecordEdge;
import net.deelam.graphtools.GraphRecordEdgeImpl;
import net.deelam.graphtools.GraphRecordImpl;

import org.apache.hadoop.io.WritableComparable;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

/**
 * @author deelam, Created:Nov 10, 2015
 */
@NoArgsConstructor // required by Hadoop
public class GraphRecordEdgeWriteable extends GraphRecordEdgeImpl implements GraphRecordEdge, WritableComparable<GraphRecordEdgeWriteable> {
  private static final long serialVersionUID = 201511100959L;

  public GraphRecordEdgeWriteable(String id, String label, String outVertex, String inVertex) {
    super(id, label, outVertex, inVertex);
  }

  @Override
  public Vertex getVertex(Direction direction) throws IllegalArgumentException {
    if (direction == Direction.OUT)
      return new GraphRecordWriteable(outVertexStringId);

    if (direction == Direction.IN)
      return new GraphRecordWriteable(inVertexStringId);

    throw new UnsupportedOperationException();
  }
  
  @Override
  public void write(final DataOutput out) throws IOException {
    GraphRecordWriteable.writeElement(id, props, out);
    //      out.writeLong(outNodeLongId);
    //      out.writeLong(inNodeLongId);

    out.writeUTF(label);
    out.writeUTF(outVertexStringId);
    out.writeUTF(inVertexStringId);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    id = GraphRecordWriteable.readElementFields(props, in);
    //      outNodeLongId=in.readLong();
    //      inNodeLongId=in.readLong();

    label = in.readUTF();
    outVertexStringId = in.readUTF();
    inVertexStringId = in.readUTF();
  }

  @Override
  public int compareTo(GraphRecordEdgeWriteable o) {
    return id.compareTo((String) o.getId());
  }

}
