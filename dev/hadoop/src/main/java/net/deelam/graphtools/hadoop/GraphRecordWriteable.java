package net.deelam.graphtools.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphRecordImpl;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import com.tinkerpop.blueprints.Edge;


/**
 * @author deelam, Created:Nov 10, 2015
 */
@Slf4j
public class GraphRecordWriteable extends GraphRecordImpl implements GraphRecord, WritableComparable<GraphRecordWriteable> {
  private static final long serialVersionUID = 201511100951L;

  public static class Factory extends GraphRecord.Factory {
    @Override
    public GraphRecordWriteable create(String id){
      return new GraphRecordWriteable(id);
    }
    @Override
    public GraphRecordWriteable create(String id, String nodeType){
      return new GraphRecordWriteable(id, nodeType);      
    }
    @Override
    public GraphRecordEdgeWriteable createEdge(String id, String label, String outVertex, String inVertex) {
      return new GraphRecordEdgeWriteable(id, label, outVertex, inVertex);
    }
  }
  
  static Factory factory=new Factory();

  protected GraphRecordWriteable(String id){
    super(id);
  }
  
  protected GraphRecordWriteable(String id, String nodeType){
    super(id, nodeType);
  }
  
  @Override
  public void write(final DataOutput out) throws IOException {
    writeElement(id, props, out);
    //out.writeLong(longId);

    writeEdges(out, inEdges);
    writeEdges(out, outEdges);
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    id=readElementFields(props, in);
    //longId=in.readLong();

    inEdges.clear(); // same instance is reused by MapReduce, so must clear entries
    readEdges(in, inEdges);
    outEdges.clear();
    readEdges(in, outEdges);
  }

  // TODO: 5: address supernode memory problem (i.e., in-memory Map of in and out edges) by splitting node.  How does Faunus/Gremlin address this?
  private static final int SUPERNODE_THRESHOLD=100000;

  private void writeEdges(DataOutput out, Map<String, Edge> edges) throws IOException {
    int size = edges.size();
    if (size > SUPERNODE_THRESHOLD) { // supernode warning
      log.warn("StringIdVertexWritable id={} has over {} edges!", getId(), size);
    }
    out.writeInt(size);
    for (Edge e : edges.values()) {
      GraphRecordEdgeWriteable writableE = (GraphRecordEdgeWriteable) e;
      writableE.write(out);
    }
  }

  private void readEdges(DataInput in, Map<String, Edge> edges) throws IOException {
    int edgeCount = in.readInt();
    for (int i = 0; i < edgeCount; ++i) {
      GraphRecordEdgeWriteable edge = new GraphRecordEdgeWriteable();
      edge.readFields(in);
      edges.put(edge.getStringId(), edge);
    }
  }

  //// -----------------------

  public int compareTo(GraphRecordWriteable o) {
    return id.compareTo((String) o.getId());
  }
  
  static void writeElement(String id, Map<String,Object> props, DataOutput out) throws IOException {
    out.writeUTF(id);
    out.writeInt(props.size());
    for (Entry<String, Object> e : props.entrySet()) {
      out.writeUTF(e.getKey());
      writePropertyValue(out, e.getKey(), e.getValue());
    }
  }

  static String readElementFields(Map<String,Object> props, DataInput in) throws IOException {
    String id = in.readUTF();
    int propCount = in.readInt();
    props.clear(); // same instance is reused by MapReduce, so must clear entries
    for (int i = 0; i < propCount; ++i) {
      String key = in.readUTF();
      Object val = readPropertyValue(in);
      if (val != null)
        props.put(key, val);
    }
    return id;
  }

  /// ===== private 

  private static final short NULL_TYPE = 0;
  private static final short BOOL_TYPE = 1;
  private static final short FLOAT_TYPE = 2;
  private static final short INT_TYPE = 3;
  private static final short LONG_TYPE = 4;
  private static final short STRING_TYPE = 5;
  private static final short DOUBLE_TYPE = 6;
  private static final short DATE_TYPE = 20;
  private static final short VALUESET_TYPE = 100;

  private static void writePropertyValue(DataOutput out, String key, Object value) throws IOException {
    if (value == null) {
      out.writeShort(NULL_TYPE);
      out.writeBoolean(false);
    } else if (value instanceof String) {
      out.writeShort(STRING_TYPE);
      out.writeUTF((String) value);
    } else if (value instanceof Boolean) {
      out.writeShort(BOOL_TYPE);
      out.writeBoolean(((Boolean) value).booleanValue());
    } else if (value instanceof Float) {
      out.writeShort(FLOAT_TYPE);
      out.writeFloat(((Float) value).floatValue());
    } else if (value instanceof Integer) {
      out.writeShort(INT_TYPE);
      WritableUtils.writeVInt(out, ((Integer) value).intValue());
    } else if (value instanceof Long) {
      out.writeShort(LONG_TYPE);
      WritableUtils.writeVLong(out, ((Long) value).longValue());
    } else if (value instanceof Double) {
      out.writeShort(DOUBLE_TYPE);
      out.writeDouble(((Double) value).doubleValue());
    } else if (value instanceof Date) {
      out.writeShort(DATE_TYPE);
      WritableUtils.writeVLong(out, ((Date) value).getTime());
    } else if (value instanceof Set) {
      out.writeShort(VALUESET_TYPE);
      //TODO: 3: use Kryo to serialize object: WritableUtils.writeCompressedByteArray(out, ((Set) value).);
      out.writeUTF(((Set) value).toString());
    } else {
      throw new UnsupportedOperationException("Unhandled property class=" + value.getClass()
          + " for key=" + key);
    }
  }

  private static Object readPropertyValue(DataInput in) throws IOException {
    short valType = in.readShort();
    switch (valType) {
      case NULL_TYPE:
        in.readBoolean();
        return null;
      case STRING_TYPE:
        return in.readUTF();
      case BOOL_TYPE:
        return in.readBoolean();
      case FLOAT_TYPE:
        return in.readFloat();
      case INT_TYPE:
        return WritableUtils.readVInt(in);
      case LONG_TYPE:
        return WritableUtils.readVLong(in);
      case DOUBLE_TYPE:
        return in.readDouble();
      case DATE_TYPE:
        return new Date(WritableUtils.readVLong(in));
      case VALUESET_TYPE:
        return in.readUTF();
      default:
        throw new UnsupportedOperationException("Unhandled property class=" + valType);
    }
  }

}
