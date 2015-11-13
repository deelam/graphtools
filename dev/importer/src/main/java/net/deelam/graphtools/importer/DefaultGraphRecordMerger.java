/**
 * 
 */
package net.deelam.graphtools.importer;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;
import net.deelam.graphtools.GraphRecordEdge;
import net.deelam.graphtools.GraphRecordMerger;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 */
@Slf4j
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

  @SuppressWarnings("unchecked")
  public static void mergeProperties(Element fromE, Element toE) {
    for (String key : fromE.getPropertyKeys()) {
      if (key.equals(IdGraph.ID)) // needed, in case this method is called for GraphElements
        continue;
      
      Object fromValue = fromE.getProperty(key);
      if (fromValue == null) {
        continue;
      }

      Object toValue = toE.getProperty(key);
      if (toValue == null) {
        toE.setProperty(key, fromValue);
        continue;
      }

      if (toValue.equals(fromValue)) { // nothing to do
        continue;
      }

      { // toValue and fromValue are not null and not equal
        /* Possible cases:
         * fromValue=val toValue=val     ==> toValue=SET_VALUE  toValue__SET=Set<?>
         * fromValue=Set<?> toValue=val  ==> toValue=SET_VALUE  toValue__SET=Set<?>
         * fromValue=val toValue=Set<?>  ==>                    toValue__SET=Set<?>
         * fromValue=Set<?> toValue=Set<?>  ==>                 toValue__SET=Set<?>
         */

        // check special SET_SUFFIX property and create a Set if needed
        String setPropertyKey = key + SET_SUFFIX;
        Set<Object> valueSet = toE.getProperty(setPropertyKey);
        if (valueSet == null) {
          valueSet = new HashSet<>();
          toE.setProperty(setPropertyKey, valueSet);
          Object existingVal = toE.getProperty(key);
          valueSet.add(existingVal);
          toE.setProperty(key, SET_VALUE);
        }
        if(fromValue.equals(SET_VALUE)){ // then 
          valueSet.addAll((Set<Object>) fromE.getProperty(setPropertyKey));
        }else{
          valueSet.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
        }
      }
    }
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
