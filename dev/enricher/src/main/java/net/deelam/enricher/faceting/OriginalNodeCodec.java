package net.deelam.enricher.faceting;

import static com.google.common.base.Preconditions.checkNotNull;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.enricher.indexing.IdMapper;
import net.deelam.graphtools.JavaSetPropertyMerger;

@RequiredArgsConstructor
@Slf4j
public class OriginalNodeCodec {
  //@Getter
  private String origIdPropKey;
  private String origIdPropKeyPrefix;


  public OriginalNodeCodec(String origIdPropKey2, IdGraph<?> graph) {
    setOrigIdPropKey(origIdPropKey2, graph);
  }
  
  private void setOrigIdPropKey(String origIdPropKey, IdGraph<?> graph) {
    this.origIdPropKey = origIdPropKey;
    checkNotNull(origIdPropKey);
    origIdPropKeyPrefix=origIdPropKey+"_";
  }

  public String getOrigId(Vertex v) {
    checkNotNull(origIdPropKey);
    checkNotNull(origIdPropKeyPrefix);
    Object origId = v.getProperty(origIdPropKey);
    if(origId==null){
      log.warn("origId=null for v={}",v);
      return null;
    }
    if(origId instanceof Long){
      int nInt=IntLongEncodeUtils.getFirstInt(((Long) origId).longValue());
      //log.warn((origIdPropKeyPrefix+nInt)+" = "+v.getProperty(origIdPropKeyPrefix+nInt));
      return v.getProperty(origIdPropKeyPrefix+nInt);
    }else if(JavaSetPropertyMerger.SET_VALUE.equals(origId)){
      return (String) origId; // TODO: handle "[multivalued]"
    }else{
      log.error("What should I return for origId="+origId+" class="+origId.getClass(), new UnsupportedOperationException());
      return null;
    }
  }
  public String getSrcGraphId(Vertex v, IdMapper graphIdMapper) {
    Object origId = v.getProperty(origIdPropKey);
    if(origId==null){
      log.warn("origId=null for v={}",v);
      return null;
    }
    if(origId instanceof Long){
      int gInt=IntLongEncodeUtils.getSecondInt(((Long) origId).longValue());
      String shortGraphId = v.getProperty(origIdPropKeyPrefix+gInt);
//      log.warn((origIdPropKeyPrefix+gInt)+" = "+shortGraphId);
//    String shortGraphId = v.getProperty(srcGraphIdPropKey);
      return graphIdMapper.longId(shortGraphId);
    }else if(JavaSetPropertyMerger.SET_VALUE.equals(origId)){
      return (String) origId; // TODO: handle "[multivalued]"
    }else{
      log.error("What should I return for "+origIdPropKey+"="+origId+" class="+origId.getClass(), new UnsupportedOperationException());
      return null;
    }
  }
  
  
  static int COUNTER=0; //FIXME: replace with valueSet.size()+1
  void setOrigId(Element newV, String origId, String shortGraphId){
    int vInt=++COUNTER, gInt=++COUNTER;
    newV.setProperty(origIdPropKeyPrefix+vInt, origId);
    newV.setProperty(origIdPropKeyPrefix+gInt, shortGraphId);
    long encodedNodeId=IntLongEncodeUtils.concat(vInt, gInt);
    newV.setProperty(origIdPropKey, encodedNodeId);
  }
}
