package net.deelam.enricher.faceting;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

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
  @Getter
  private String graphIdPropKey;

  final PropertyMerger pm;

  public OriginalNodeCodec(String originalIdPropKeyPrefix, IdGraph<?> graph, PropertyMerger pm) {
    this.pm = pm;
    setOrigIdPropKey(originalIdPropKeyPrefix, graph);
  }

  private void setOrigIdPropKey(String origIdPropKeyPrefix, IdGraph<?> graph) {
    checkNotNull(origIdPropKeyPrefix);
    this.origIdPropKey = origIdPropKeyPrefix + PropertyMerger.VALUELIST_SUFFIX;
    graphIdPropKey = origIdPropKeyPrefix + "graph_" + PropertyMerger.VALUELIST_SUFFIX;
  }

  public String getOrigId(Vertex v) {
    checkNotNull(origIdPropKey);
    checkNotNull(graphIdPropKey);

    Object origId = v.getProperty(origIdPropKey);
    if(origId==null)
      return null;
    if (pm.isMultivalued(origId)) {
      log.warn("What should I return for " + origIdPropKey + "=" + origId);
      return null;
    } else {
      return (String) origId;
    }
  }

  public String getSrcGraphId(Vertex v, IdMapper graphIdMapper) {
    Object srcGraphId = v.getProperty(graphIdPropKey);
    if(srcGraphId==null)
      return null;
    if (pm.isMultivalued(srcGraphId)) {
      log.warn("What should I return for " + graphIdPropKey + "=" + srcGraphId);
      return null;
    } else {
      return graphIdMapper.longId((String) srcGraphId);
    }
  }

  void setOrigId(Element newV, String origId, String shortGraphId) {
    newV.setProperty(origIdPropKey, origId);
    newV.setProperty(graphIdPropKey, shortGraphId);
  }
  
  public void removeOrigNodeProperties(Vertex v) {
    v.removeProperty(origIdPropKey);
    v.removeProperty(graphIdPropKey);
  }

  public List<String> getOrigNodeIdList(Vertex v) {
    Object origId = v.getProperty(origIdPropKey);
    if(origId==null)
      return null;
//    if (pm.isMultivalued(origId))
    return pm.getListProperty(v, origIdPropKey);
  }

  public List<String> getSrcGraphIdList(Vertex v, IdMapper graphIdMapper) {
    Object srcGraphId = v.getProperty(graphIdPropKey);
    if(srcGraphId==null)
      return null;
    
    
    List<String> shortGraphIds=pm.getListProperty(v, graphIdPropKey);
    List<String> list=new ArrayList<>();
    for(String shortGraphId:shortGraphIds){
      String longId = graphIdMapper.longId((String) shortGraphId);
      checkNotNull(longId);
      list.add(longId);
    }
    return list;
  }

}
