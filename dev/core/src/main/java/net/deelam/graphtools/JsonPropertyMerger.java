package net.deelam.graphtools;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;

import org.boon.core.value.CharSequenceValue;
import org.boon.core.value.NumberValue;
import org.boon.core.value.ValueList;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author deelam
 */
@RequiredArgsConstructor
@Slf4j
public class JsonPropertyMerger implements PropertyMerger {

  @Override
  public void mergeProperties(Element fromE, Element toE) {
    for (String key : fromE.getPropertyKeys()) {
      if (key.equals(IdGraph.ID)) // needed, in case this method is called for GraphElements
        continue;

      Object fromValue = fromE.getProperty(key);
      if (fromValue == null) {
        continue;
      }

      // fromValue is not null at this point
      Object toValue = toE.getProperty(key);
      if (toValue == null) {
        setElementProperty(toE, key, fromValue);
        if (isMultivalued(fromValue)) {
          Object fromValueSet = fromE.getProperty(key);
          if(fromValueSet instanceof Set){ // created by JavaSetPropertyMerger
            toE.setProperty(key, mapper.toJson(fromValueSet));
          } else {
            String fromValueSetStr = (String) fromValueSet;
            toE.setProperty(key, fromValueSetStr);
          }
        }
        continue;
      } else if (!key.endsWith(VALUELIST_SUFFIX) && toValue.equals(fromValue)) {
        // nothing to do; values are the same
        continue;
      } else try {
        mergeValues(fromValue, toE, key);
      } catch (Exception e) {
        log.warn("Could not merge property values for key=" + key, e);
      }
    } // for loop
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
    
    //Titan supported property types: http://s3.thinkaurelius.com/docs/titan/0.5.0/schema.html#_defining_property_keys
  }

  boolean isAllowedValue(Object value){
    return (value.getClass().isPrimitive() || validPropertyClasses.contains(value.getClass()));
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
      elem.setProperty(key, value); // TODO: 6: for Titan, must use addProperty() to store as list or set if value is a Collection
    } else { // save as Json
      elem.setProperty(key, mapper.toJson(value));
    }
  }

  private ObjectMapper mapper = JsonFactory.create();
  private static final Set<String> allowedMultivaluedProps=new LinkedHashSet<>();
  public static void allowMultivaluedProperty(String propName){
    allowedMultivaluedProps.add(propName);
  }

  public void mergeValues(Object fromValue, Element toE, String key) {
    // toValue and fromValue are not null and want to add fromValue to the list
    /* Possible cases:
     * fromValue=val toValue=val     ==> toValue__SET=Set<?>
     * fromValue=Set<?> toValue=val  ==> toValue__SET=Set<?>
     * fromValue=val toValue=Set<?>  ==> toValue__SET=Set<?>
     * fromValue=Set<?> toValue=Set<?>  ==> toValue__SET=Set<?>
     */

    // check special SET_SUFFIX property and create a Set if needed
    Object value = toE.getProperty(key);
    Object parsedV =value;
    if(value instanceof String)
      parsedV = parseValue((String) value);
    List valueList=null;
    if (!isAllowedValue(parsedV)) {
      valueList = (List) parsedV;
    }

    if (valueList == null) {
      valueList = new ValueList(false);
      Object existingVal = toE.getProperty(key);
      valueList.add(existingVal);
      if(!allowedMultivaluedProps.contains(key)){
        log.warn("Property has multiple values which is inefficient: key="+key+" for node="+toE.getId()
            + " existingVal="+existingVal+" newValue="+fromValue/*, new Throwable("Call stack")*/);
      }
    }

    /// check if fromValue is already in toValueList
    boolean toValueChanged = false;
    if (isMultivalued(fromValue)) {
      String fromListStr = (String) fromValue; //fromE.getProperty(valSetPropKey);
      List fromValueList = (List) parseValue(fromListStr); //parseList(compClass, fromListStr);
      for (Object fVal : fromValueList) {
        if(key.endsWith(VALUELIST_SUFFIX)){
          valueList.add(fVal); // hopefully, fromValue is the same type as other elements in the set
          toValueChanged = true;
        } else { // treat as a Set 
          if (!valueList.contains(fVal)) { // not efficient since searching in a list
//            if (!compClass.equals(fVal.getClass()))
//              log.warn("existingClass={} newValueClass={}", compClass, fVal.getClass());
            valueList.add(fVal); // hopefully, fromValue is the same type as other elements in the set
            toValueChanged = true;
          }
        }
      }
    } else { // fromValue is a single value
      if(key.endsWith(VALUELIST_SUFFIX)){
        valueList.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
        toValueChanged = true;
      } else { // treat as a Set 
        if (!valueList.contains(fromValue)) {
//          if (!compClass.equals(fromValue.getClass()))
//            log.warn("existingClass={} newValueClass={}", compClass, fromValue.getClass());
          valueList.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
          toValueChanged = true;
        }
      }
    }

    if (toValueChanged)
      toE.setProperty(key, mapper.toJson(valueList));
  }

  private Object parseValue(String jsonStr) {
    if(jsonStr==null)
      return null;
    try{
      if(jsonStr.length()==0)
        return jsonStr;
      Object parsedV = mapper.parser().parse(jsonStr);
      if(parsedV instanceof NumberValue)
        return ((NumberValue) parsedV).toValue();
      else if(parsedV instanceof CharSequenceValue)
        return jsonStr;
      else
        return parsedV;
    }catch(RuntimeException e){
      log.error("jsonStr="+jsonStr, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean addProperty(Element elem, String key, Object value) {
    if(isMultivalued(value))
      throw new IllegalArgumentException("TODO: allow adding multivalued value?");
    Object existingVal = elem.getProperty(key);
    if(existingVal==null){
      elem.setProperty(key, value);
      return true;
    }else{
      mergeValues(existingVal, elem, key);
      return false;
    }
  }
  
  @Override
  public boolean isMultivalued(Object value) {
    if(value==null)
      return false;
    if (value instanceof String) {
      Object parsedV = parseValue((String) value);
      return !isAllowedValue(parsedV);
    } else if (value instanceof Collection) {
      return true;
    } else if (isAllowedValue(value)) {
      return false;
    } else {
      throw new IllegalStateException("Wasn't expecting value of class=" + value.getClass() + " value=" + value);
    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public <T> List<T> getArrayProperty(Element elem, String key) {
    final Object valueSet = elem.getProperty(key);
    if (valueSet == null) {
      Object val = elem.getProperty(key);
      if(val==null)
        return null;
      else {
        List<T> arr = new ArrayList<>();
        arr.add((T) val);
        return arr;
      }
    }else{
      String valueSetStr=(String) valueSet;
      try{
        List<T> valueList = (List<T>) mapper.parser().parse(valueSetStr);
        return valueList;
      }catch(Exception e){
        log.error("key="+key+" for node="+elem.getId()+" valueSetStr="+valueSetStr, e);
        throw new RuntimeException(e);
      }
    }
  }
  
  @Override
  public int getArrayPropertySize(Element elem, String key){
    final Object valueSet = elem.getProperty(key);
    if (valueSet == null) {
      Object val = elem.getProperty(key);
      if(val==null)
        return 0;
      else {
        return 1;
      }
    }else{
      String valueSetStr=(String) valueSet;
      try{
        List<?> valueList = (List<?>) mapper.parser().parse(valueSetStr);
        return valueList.size();
      }catch(Exception e){
        log.error("key="+key+" for node="+elem.getId() +" valueSetStr="+valueSetStr, e);
        throw new RuntimeException(e);
      }
    }
  }
  // TODO: 3: set limit on size of Set
  // TODO: 3: add supernode detection and warning

  public Map<String, Object> convertToJson(Map<String, Object> props){
    Map<String, Object> newMap=new HashMap<>();
    for(Entry<String, Object> e:props.entrySet()){
      if (isMultivalued(e.getValue())) { 
        newMap.put(e.getKey(), mapper.toJson(e.getValue()));
      }else if (validPropertyClasses.contains(e.getValue().getClass())) {
        newMap.put(e.getKey(), e.getValue());
      } else {// save as Json
        newMap.put(e.getKey(), mapper.toJson(e.getValue()));
      }
    }
    return newMap;
  }

  public void convertFromJson(Map<String, Object> existingProps, GraphRecord tempGr) {
    for(Entry<String, Object> entry:existingProps.entrySet()){
      Object value = entry.getValue();
      if(isMultivalued(value)){
        try{
          if(value instanceof String)
            value=parseValue((String) value);
          Collection<?> col;
          if(entry.getKey().endsWith(VALUELIST_SUFFIX)){
            col=(List<?>) value;
          }else{
            col=new LinkedHashSet<>((List<?>) value); //mapper.parser().parse(valueStr));
          }
          tempGr.setProperty(entry.getKey(), col);
        }catch(Exception e){
          log.error("{} {} {}", entry.getValue(), entry.getValue().getClass(), value);
          throw e;
        }
      }else{
        tempGr.setProperty(entry.getKey(), value);
      }
    }
  }
  
  
  ///========================================
  
  @SuppressWarnings({"rawtypes","unchecked"})
  public static void main(String[] args) throws ClassNotFoundException {
    System.out.println(Integer.class.isPrimitive());
    System.out.println(int.class.isPrimitive());
    
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
      LinkedHashSet<Number> valueSet = new LinkedHashSet<>();
      valueSet.add(1L);
      valueSet.add(2.0);
      valueSet.add(2.0f);
      valueSet.add(1);
      String intArrJson = mapper.toJson(valueSet);
      System.out.println(intArrJson);
      System.out.println(mapper.parser().parse(intArrJson).getClass());
    }

    {
      LinkedHashSet<Object> valueSet = new LinkedHashSet<>();
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
      LinkedHashSet<Date> valueSet = new LinkedHashSet<>();
      valueSet.add(new Date());
      valueSet.add(new Date());
      String inJson = mapper.toJson(valueSet);
      System.out.println(inJson);
      List list = (List) mapper.parser().parse(inJson);
      System.out.println(list.get(0).getClass());
    }
    {
      LinkedHashSet<Object> valueSet = new LinkedHashSet<>();
      valueSet.add(new JsonPropertyMerger());
      String inJson = mapper.toJson(valueSet);
      System.out.println(inJson);
      List list = (List) mapper.parser().parse(inJson);
      System.out.println(list.get(0).getClass());
    }
  }

}
