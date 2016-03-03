/**
 * 
 */
package net.deelam.graphtools;

import java.util.HashSet;
import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 */
//@Slf4j
public class JavaSetPropertyMerger implements PropertyMerger {

  static final String VALUE_CLASS_SUFFIX = "__jsonClass";
  static final String SET_VALUE = "[multivalued]";
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
          toE.setProperty(setPropertyKey, new HashSet<>(fromValueSet));
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
          valueSet = new HashSet<>();
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

}
