/**
 * 
 */
package net.deelam.graphtools;

import java.lang.reflect.Array;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.boon.core.value.ValueList;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 */
@Slf4j
public class JsonPropertyMerger implements PropertyMerger {

  private static final String VALUE_CLASS_SUFFIX = "__jsonClass";
  static final String SET_VALUE = "[multivalued]";
  static final String SET_SUFFIX = "__jsonSET";

  @Override
  public void mergeProperties(Element fromE, Element toE) {
    for (String key : fromE.getPropertyKeys()) {
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
      if (toValue == null) {
        setElementProperty(toE, key, fromValue);
        if (fromValue.equals(SET_VALUE)) {
          String setPropertyKey = key + SET_SUFFIX;
          String fromValueSetStr = fromE.getProperty(setPropertyKey);
          toE.setProperty(setPropertyKey, fromValueSetStr);
        }
        continue;
      }

      if (!fromValue.equals(SET_VALUE) && toValue.equals(fromValue)) {
        // nothing to do; values are the same
        continue;
      }

      try {
        mergeProperty(fromE, toE, key, fromValue);
      } catch (ClassNotFoundException e) {
        log.warn("Could not merge property values for key=" + key, e);
      }
    }
  }

  @Getter
  private final Set<Class<?>> validPropertyClasses = new HashSet<>();
  {
    // this list is created from Neo4j's PropertyStore class
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

  // for Graphs (like Neo4j) that can only store primitives
  private void setElementProperty(Element elem, String key, Object value) {
    Object leafValue;
    if (value.getClass().isArray()) {
      if (Array.getLength(value) == 0) {
        elem.setProperty(key, value);
        return;
      } else {
        leafValue = Array.get(value, 0);
      }
    } else {
      leafValue = value;
    }

    if (validPropertyClasses.contains(leafValue.getClass())) {
      elem.setProperty(key, value);
    } else { // save as Json
      elem.setProperty(key, mapper.toJson(value));
      setPropertyValueClass(elem, key, value);
    }
  }

  private void setPropertyValueClass(Element elem, String key, Object value) {
    final String compClassPropKey = key + VALUE_CLASS_SUFFIX;
    elem.setProperty(compClassPropKey, value.getClass().getCanonicalName());
  }

  private Class<?> getPropertyValueClass(Element elem, String key) throws ClassNotFoundException {
    final String compClassPropKey = key + VALUE_CLASS_SUFFIX;
    String classStr = elem.getProperty(compClassPropKey);
    if (classStr == null)
      return null;
    return Class.forName(classStr);
  }

  private ObjectMapper mapper = JsonFactory.create();

  public void mergeProperty(Element fromE, Element toE, String key, Object fromValue) throws ClassNotFoundException {
    // toValue and fromValue are not null and not equal
    /* Possible cases:
     * fromValue=val toValue=val     ==> toValue=SET_VALUE  toValue__SET=Set<?>
     * fromValue=Set<?> toValue=val  ==> toValue=SET_VALUE  toValue__SET=Set<?>
     * fromValue=val toValue=Set<?>  ==>                    toValue__SET=Set<?>
     * fromValue=Set<?> toValue=Set<?>  ==>                 toValue__SET=Set<?>
     */

    // check special SET_SUFFIX property and create a Set if needed
    final String valSetPropKey = key + SET_SUFFIX;
    final String valueSetStr = toE.getProperty(valSetPropKey);
    List valueList;

    Class<?> compClass = getPropertyValueClass(toE, key);

    if (valueSetStr == null) {
      valueList = new ValueList(false);
      Object existingVal = toE.getProperty(key);
      valueList.add(existingVal);
      toE.setProperty(key, SET_VALUE);

      if (compClass == null) {
        setPropertyValueClass(toE, key, existingVal);
        compClass = existingVal.getClass();
      }

    } else {
      valueList = (List) mapper.parser().parseList(compClass, valueSetStr);
    }

    /// check if fromValue is already in toValueList
    boolean toValueChanged = false;
    if (fromValue.equals(SET_VALUE)) {
      String fromListStr = fromE.getProperty(valSetPropKey);
      List fromValueList = (List) mapper.parser().parseList(compClass, fromListStr);
      for (Object fVal : fromValueList) {
        if (!valueList.contains(fVal)) {
          if (!compClass.equals(fVal.getClass()))
            log.warn("existingClass={} newValueClass={}", compClass, fVal.getClass());
          valueList.add(fVal); // hopefully, fromValue is the same type as other elements in the set
          toValueChanged = true;
        }
      }
    } else { // fromValue is a single value
      if (!valueList.contains(fromValue)) {
        if (!compClass.equals(fromValue.getClass()))
          log.warn("existingClass={} newValueClass={}", compClass, fromValue.getClass());
        valueList.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
        toValueChanged = true;
      }
    }

    if (toValueChanged)
      toE.setProperty(valSetPropKey, mapper.toJson(valueList));
  }
  
  public Map<String, Object> convertToJson(Map<String, Object> props){
    Map<String, Object> newMap=new HashMap<>();
    for(Entry<String, Object> e:props.entrySet()){
      if (validPropertyClasses.contains(e.getValue().getClass())) {
        newMap.put(e.getKey(), e.getValue());
      } else { // save as Json
        newMap.put(e.getKey(), SET_VALUE);
        
        final String compClassPropKey = e.getKey() + VALUE_CLASS_SUFFIX;
        newMap.put(compClassPropKey, e.getValue().getClass().getCanonicalName());
        
        String setPropertyKey = e.getKey() + SET_SUFFIX;
        newMap.put(setPropertyKey, mapper.toJson(e.getValue()));
      }
    }
    return newMap;
  }
  

  public void convertFromJson(Map<String, Object> existingProps, GraphRecord tempGr) {
    for(Entry<String, Object> entry:existingProps.entrySet()){
      if(entry.getKey().equals(SET_VALUE)){
        Class<?> compClass;
        {
          final String compClassPropKey = entry.getKey() + VALUE_CLASS_SUFFIX;
          String classStr = (String) existingProps.get(compClassPropKey);
          try {
            compClass=Class.forName(classStr);
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        }
        String valueSetStr;
        final String valSetPropKey = entry.getKey() + SET_SUFFIX;
        {
          valueSetStr = (String) existingProps.get(valSetPropKey);
        }
        Set set=new HashSet((List) mapper.parser().parseList(compClass, valueSetStr));
        tempGr.setProperty(entry.getKey(), entry.getValue());
        tempGr.setProperty(valSetPropKey, set);
      }else if(entry.getKey().endsWith(SET_SUFFIX)){
        // ignore
      }else{
        tempGr.setProperty(entry.getKey(), entry.getValue());
      }
    }
  }

  public static void main(String[] args) throws ClassNotFoundException {
    ObjectMapper mapper = JsonFactory.create();
    //    String jsonArray = "[0,1,2,3,4,5,6,7,8,7]";
    //    Integer[] intArray = mapper.parser().parseIntArray( jsonArray );

    {
      String json = mapper.toJson("Hi");
      System.out.println(json);
      System.out.println(mapper.parser().parse(String.class, json).getClass());
    }
    {
      String json = mapper.toJson(Integer.valueOf(1).getClass().getCanonicalName());
      System.out.println(json);
      System.out.println(Class.forName(mapper.parser().parseString(json)));
    }
    {
      String json = mapper.toJson(1L);
      System.out.println(json);
      System.out.println(mapper.parser().parse(json).getClass());
    }
    {
      HashSet<Number> valueSet = new HashSet<>();
      valueSet.add(1L);
      valueSet.add(2.0);
      valueSet.add(2.0f);
      valueSet.add(1);
      String intArrJson = mapper.toJson(valueSet);
      System.out.println(intArrJson);
      System.out.println(mapper.parser().parse(intArrJson).getClass());
    }

    {
      HashSet<Object> valueSet = new HashSet<>();
      valueSet.add("a");
      valueSet.add("2");
      valueSet.add("b");
      valueSet.add('b');
      valueSet.add(true);
      valueSet.add(2234567892345678L);
      String inJson = mapper.toJson(valueSet);
      System.out.println(inJson);
      List list = (List) mapper.parser().parse(inJson);
      if (!list.contains("c"))
        list.add("c");
      if (!list.contains("c"))
        list.add("d");
      System.out.println(list);
      for(Object i:list){
        System.err.println(i.getClass());
      }
    }
    {
      HashSet<Date> valueSet = new HashSet<>();
      valueSet.add(new Date());
      valueSet.add(new Date());
      String inJson = mapper.toJson(valueSet);
      System.out.println(inJson);
      List list = (List) mapper.parser().parse(inJson);
      System.out.println(list.get(0).getClass());
    }
    {
      HashSet<Object> valueSet = new HashSet<>();
      valueSet.add(new JsonPropertyMerger());
      String inJson = mapper.toJson(valueSet);
      System.out.println(inJson);
      List list = (List) mapper.parser().parse(inJson);
      System.out.println(list.get(0).getClass());
    }
  }

}
