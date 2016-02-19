package net.deelam.graphtools.importer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphRecordEdge;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * Not thread-safe
 * @author deelam, Created:Nov 10, 2015
 */
@RequiredArgsConstructor
@Slf4j
public class GraphRecordBuilder<B> {
  private final Encoder<B> encoder;
  private final GraphRecord.Factory grFactory;

  private Map<String, GraphRecord> gRecords = new HashMap<>(40);

  @SuppressWarnings("unchecked")
  public Collection<GraphRecord> build(B bean) {
    RecordContext<B> rContext = encoder.createContext(bean);
    int numRelRules = encoder.getEntityRelationCount(rContext);
    // reuse map for efficiency, assumes this method is called for each bean, one at a time
    // which is true in the above implementation
    gRecords.clear();
    for (int i = 0; i < numRelRules; ++i) {
      @SuppressWarnings("rawtypes")
      EntityRelation relation = encoder.getEntityRelation(i, rContext);
      NodeFiller<RecordContext<B>> srcNodeFiller = relation.srcNodeFiller;
      NodeFiller<RecordContext<B>> dstNodeFiller = relation.dstNodeFiller;
      EdgeFiller<RecordContext<B>> edgeFiller = relation.edgeFiller;
      int numInstances = relation.numInstances(rContext);

      for (int j = 0; j < numInstances; ++j) {
        rContext.setInstanceIndex(j);
        GraphRecord outFv = null;
        {
          String outVertexId = srcNodeFiller.getId(rContext);
          if (outVertexId != null) {
            outFv = gRecords.get(outVertexId);
            if (outFv == null) {
              outFv = grFactory.create(outVertexId, srcNodeFiller.getType());
              outFv.setProperty(IdGraph.ID, outVertexId);
              gRecords.put(outVertexId, outFv);
            }
            srcNodeFiller.fill(outFv, rContext);
          }
        }

        GraphRecord inFv = null;
        if (dstNodeFiller != null) {
          String inVertexId = dstNodeFiller.getId(rContext);
          if (inVertexId != null) {
            inFv = gRecords.get(inVertexId);
            if (inFv == null) {
              inFv = grFactory.create(inVertexId, dstNodeFiller.getType());
              inFv.setProperty(IdGraph.ID, inVertexId);
              gRecords.put(inVertexId, inFv);
            }
            dstNodeFiller.fill(inFv, rContext);
          }

          String label = edgeFiller.getLabel();
          if (outFv != null && inFv != null) {
            String edgeId = edgeFiller.getId(outFv, inFv, rContext);
            if (label != null && edgeId != null) {
              GraphRecordEdge fe = outFv.getOutEdge(edgeId);
              if (fe == null) {
                fe = grFactory.createEdge(edgeId, label, outFv, inFv);
                fe.setProperty(IdGraph.ID, edgeId);
                //fe.setProperty(CsvGraphFiller.LONG_ID_PROPKEY, generateEdgeId());
                outFv.addEdge(fe);
                //log.info("outEdges: "+Iterables.toString(outFv.getEdges(Direction.BOTH)));
                //log.info("Creating edge: "+fe);

                // use an empty edge to reduce merging properties later
                //GraphRecordEdge feEmpty=new GraphRecordEdge(label, outFv, inFv);
                if (!outFv.equals(inFv)) // if edge is self-edge, it has already been added to both IN and OUT edge tables
                  inFv.addEdge(fe/*.emptyCopy()*/);
                //log.info("inEdges: "+Iterables.toString(inFv.getEdges(Direction.BOTH)));
              } else {
                log.debug("Using existing edge: {}", edgeId);
                Preconditions.checkState(fe.getOutVertexStringId().equals(outFv.getStringId()));
                Preconditions.checkState(fe.getInVertexStringId().equals(inFv.getStringId()));
              }
              edgeFiller.fill(fe, rContext);
            }
          }
        }
      }
    }
    Collection<GraphRecord> records = gRecords.values();
    convertPropertyValues(records);
    return records;
  }

  @Getter
  private final static Set<Class<?>> validPropertyClasses = new HashSet<>();
  {
    validPropertyClasses.add(String.class);
    validPropertyClasses.add(Integer.class);
    validPropertyClasses.add(Boolean.class);
    validPropertyClasses.add(Float.class);
    validPropertyClasses.add(Long.class);
    validPropertyClasses.add(Double.class);
    validPropertyClasses.add(Byte.class);
    validPropertyClasses.add(Character.class);
    validPropertyClasses.add(Short.class);
  }
  
  private void convertPropertyValues(Collection<GraphRecord> records) {
    for(GraphRecord rec:records){
      convertProperties(rec);
      for(Edge e:rec.getInEdges().values()){
        convertProperties(e);
      }
      for(Edge e:rec.getOutEdges().values()){
        convertProperties(e);
      }
    }
  }

  Collection<String> keys=new ArrayList<>();
  private void convertProperties(Element elem) {
    keys.clear();
    for(String key:elem.getPropertyKeys()){
      Object val = elem.getProperty(key);
      if(val!=null && !validPropertyClasses.contains(val.getClass())){
        if(val instanceof Date){
          keys.add(key);
        } else {
          log.warn("Unhandled property value of class="+val.getClass()+" "+val);
        }
      }
    }
    for(String key:keys){
      Object val = elem.getProperty(key);
      elem.setProperty(key, ((Date)val).getTime()); // use this to deserialize
      elem.setProperty(key+"_string", val.toString()); // this is for readability
    }
  }

  public static void main(String[] args) {
    Object i=1d;
    System.out.println(i.getClass());
    System.out.println((i instanceof String));
    System.out.println((i instanceof Integer));
    System.out.println((i instanceof Long));
    System.out.println((i instanceof Float));
    System.out.println((i instanceof Boolean));
  }
}
