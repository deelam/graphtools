package net.deelam.graphtools.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import net.deelam.graphtools.GraphRecordEdge;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author deelam, Created:Nov 10, 2015
 */
public class GraphRecordEdgeWriteable extends GraphRecordEdge implements
    WritableComparable<GraphRecordEdgeWriteable> {
  private static final long serialVersionUID = 201511100959L;

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
