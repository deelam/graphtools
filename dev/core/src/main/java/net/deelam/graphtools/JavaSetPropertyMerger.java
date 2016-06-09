/**
 * 
 */
package net.deelam.graphtools;

import java.util.LinkedHashSet;
import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.RequiredArgsConstructor;

/**
 * @author deelam
 */
@RequiredArgsConstructor
//@Slf4j
public class JavaSetPropertyMerger implements PropertyMerger {

  static final String VALUE_CLASS_SUFFIX = "__jsonClass";
  public static final String SET_VALUE = "[multivalued]";
  static final String SET_SUFFIX = "__jsonSET";

  @SuppressWarnings("unchecked")
  public void mergeProperties(Element fromE, Element toE) {
    for (String key : fromE.getPropertyKeys()) {
      if(key.length()==0)
        throw new IllegalArgumentException("Property key cannot be empty: "+fromE);
      
      if (key.equals(IdGraph.ID)) // needed, in case this method is called for GraphElements
        continue;

      if (key.endsWith(SET_SUFFIX)) {
        continue;
      }

      Object fromValue = fromE.getProperty(key);
      if (fromValue == null) {
        continue;
      }

      // fromValue is not null at this point

      Object toValue = toE.getProperty(key);
      if (toValue == null) { // toNode does not have value
        toE.setProperty(key, fromValue);
        if (fromValue.equals(SET_VALUE)) {
          String setPropertyKey = key + SET_SUFFIX;
          Set<Object> fromValueSet = (Set<Object>) fromE.getProperty(setPropertyKey);
          toE.setProperty(setPropertyKey, new LinkedHashSet<>(fromValueSet));
        }
        continue;
      }

      if (!fromValue.equals(SET_VALUE) && toValue.equals(fromValue)) {
        // nothing to do; values are the same
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
          valueSet = new LinkedHashSet<>();
          toE.setProperty(setPropertyKey, valueSet);
          Object existingVal = toE.getProperty(key);
          valueSet.add(existingVal);
          toE.setProperty(key, SET_VALUE);
          //setPropertyValueClass(toE, key, existingVal);
        }
        if (fromValue.equals(SET_VALUE)) { // then 
          valueSet.addAll((Set<Object>) fromE.getProperty(setPropertyKey));
        } else {
          valueSet.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
        }
      }
    }
  }
  
  
  private void setPropertyValueClass(Element elem, String key, Object value) {
    final String compClassPropKey = key + VALUE_CLASS_SUFFIX;
    elem.setProperty(compClassPropKey, value.getClass().getCanonicalName());
  }


  @Override
  public Object[] getArrayProperty(Element elem, String key) {
    String setPropertyKey = key + SET_SUFFIX;
    Set<Object> valueSet = elem.getProperty(setPropertyKey);
    if (valueSet == null) {
      Object val = elem.getProperty(key);
      if(val==null)
        return null;
      else {
        Object[] arr = new Object[1];
        arr[0]=val;
        return arr;
      }
    }else{
      return valueSet.toArray(new Object[valueSet.size()]);
    }
  }
  
  @Override
  public int getArrayPropertySize(Element elem, String key){
    String setPropertyKey = key + SET_SUFFIX;
    Set<Object> valueSet = elem.getProperty(setPropertyKey);
    if (valueSet == null) {
      Object val = elem.getProperty(key);
      if(val==null)
        return 0;
      else {
        return 1;
      }
    }else{
      return valueSet.size();
    }
  }

}
