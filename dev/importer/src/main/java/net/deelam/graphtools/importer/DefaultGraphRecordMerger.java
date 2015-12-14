/**
 * 
 */
package net.deelam.graphtools.importer;

import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Inject;

import lombok.Setter;
import net.deelam.graphtools.*;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;

/**
 * @author deelam
 */
//@Slf4j
public class DefaultGraphRecordMerger implements GraphRecordMerger {

  @Override
  public void merge(GraphRecord from, GraphRecord to) {
    Preconditions.checkState(from.getType().equals(to.getType()), "Records have equal IDs but different types!");
    mergeProperties(from, to);
    merge(from.getOutEdges(), to.getOutEdges());
    merge(from.getInEdges(), to.getInEdges());
  }

  static final String SET_VALUE = "[multivalued]";
  static final String SET_SUFFIX = "__SET";

  @Setter
  private PropertyMerger propertyMerger;
  
  public DefaultGraphRecordMerger() {
    this.propertyMerger = new JavaSetPropertyMerger();
  }

  @Inject
  public DefaultGraphRecordMerger(PropertyMerger propertyMerger) {
    this.propertyMerger = propertyMerger;
  }


  public void mergeProperties(Element fromE, Element toE) {
    propertyMerger.mergeProperties(fromE, toE);
  }
  

  private void merge(Map<String, Edge> fromEdges, Map<String, Edge> toEdges) {
    for(Entry<String, Edge> entry:fromEdges.entrySet()){
      Edge fromEdge = entry.getValue();
      Edge toEdge = toEdges.get(entry.getKey());
      if(toEdge==null){
        toEdges.put(entry.getKey(), fromEdge);
      }else{
        merge((GraphRecordEdge) fromEdge, (GraphRecordEdge) toEdge);
      }
    }
  }


  private void merge(GraphRecordEdge fromEdge, GraphRecordEdge toEdge) {
    // check label, inV, outV are equal
    Preconditions.checkState(fromEdge.getLabel().equals(toEdge.getLabel()), 
        "Records have equal IDs but different labels!");
    Preconditions.checkState(fromEdge.getOutVertexStringId().equals(toEdge.getOutVertexStringId()), 
        "Records have equal IDs but different out vertices!");
    Preconditions.checkState(fromEdge.getInVertexStringId().equals(toEdge.getInVertexStringId()), 
        "Records have equal IDs but different in vertices!");
    
    mergeProperties(fromEdge, toEdge);
  }

}
