package net.deelam.enricher.faceting;

import static com.google.common.base.Preconditions.checkNotNull;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.enricher.indexing.IdMapper;
import net.deelam.graphtools.PropertyMerger;

@RequiredArgsConstructor
@Slf4j
public class OriginalNodeCodec {
  @Getter
  private String origIdPropKey;
  private String graphIdPropKey;
  
  final PropertyMerger pm;

  public OriginalNodeCodec(String originalIdPropKeyPrefix, IdGraph<?> graph, PropertyMerger pm) {
    this.pm=pm;
    setOrigIdPropKey(originalIdPropKeyPrefix, graph);
  }
  
  private void setOrigIdPropKey(String origIdPropKeyPrefix, IdGraph<?> graph) {
    checkNotNull(origIdPropKeyPrefix);
    this.origIdPropKey = origIdPropKeyPrefix+PropertyMerger.VALUELIST_SUFFIX;
    graphIdPropKey=origIdPropKeyPrefix+"graph_"+PropertyMerger.VALUELIST_SUFFIX;
  }

  public String getOrigId(Vertex v) {
    checkNotNull(origIdPropKey);
    checkNotNull(graphIdPropKey);
    
    Object origId = v.getProperty(origIdPropKey);
    if(pm.isMultivalued(origId)){
      // TODO: handle "[multivalued]"
      log.error("What should I return for " + origIdPropKey + "=" + origId, new UnsupportedOperationException());
      return null;
    }else{
      return (String) origId;
    }

//    Object origId = v.getProperty(origIdPropKey);
//    if(origId==null){
//      log.warn("origId=null for v={}",v);
//      return null;
//    }
//    if(origId instanceof Long){
//      int nInt=IntLongEncodeUtils.getFirstInt(((Long) origId).longValue());
//      //log.warn((origIdPropKeyPrefix+nInt)+" = "+v.getProperty(origIdPropKeyPrefix+nInt));
//      return v.getProperty(origIdPropKeyPrefix+nInt);
//    }else if(JavaSetPropertyMerger.SET_VALUE.equals(origId)){
//      return (String) origId; // TODO: handle "[multivalued]"
//    }else{
//      log.error("What should I return for origId="+origId+" class="+origId.getClass(), new UnsupportedOperationException());
//      return null;
//    }
  }
  public String getSrcGraphId(Vertex v, IdMapper graphIdMapper) {
    Object srcGraphId = v.getProperty(graphIdPropKey);
    if(pm.isMultivalued(srcGraphId)){
      // TODO: handle "[multivalued]"
      log.error("What should I return for " + graphIdPropKey + "=" + srcGraphId, new UnsupportedOperationException());
      return null;
    }else{
      return graphIdMapper.longId((String) srcGraphId);
    }
    
//    Object origId = v.getProperty(origIdPropKey);
//    if(origId==null){
//      log.warn("origId=null for v={}",v);
//      return null;
//    }
//    if(origId instanceof Long){
//      int gInt=IntLongEncodeUtils.getSecondInt(((Long) origId).longValue());
//      String shortGraphId = v.getProperty(origIdPropKeyPrefix+gInt);
////      log.warn((origIdPropKeyPrefix+gInt)+" = "+shortGraphId);
////    String shortGraphId = v.getProperty(srcGraphIdPropKey);
//      return graphIdMapper.longId(shortGraphId);
//    }else if(JavaSetPropertyMerger.SET_VALUE.equals(origId)){
//      return (String) origId; // TODO: handle "[multivalued]"
//    }else{
//      log.error("What should I return for "+origIdPropKey+"="+origId+" class="+origId.getClass(), new UnsupportedOperationException());
//      return null;
//    }
  }
  
  void setOrigId(Element newV, String origId, String shortGraphId){
    newV.setProperty(origIdPropKey, origId);
    newV.setProperty(graphIdPropKey, shortGraphId);
  }
}
