/**
 * 
 */
package net.deelam.graphtools.importer;

import java.util.Date;
import java.util.HashSet;

import org.boon.core.value.ValueList;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 */
//@Slf4j
public class JsonPropertyMerger implements PropertyMerger {

  static final String SET_VALUE = "[multivalued]";
  static final String SET_SUFFIX = "__jsonSET";

  @Override
  public void mergeProperties(Element fromE, Element toE) {
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

      mergeProperty(fromE, toE, key, fromValue);
    }
  }

  private ObjectMapper mapper = JsonFactory.create();
  public void mergeProperty(Element fromE, Element toE, String key, Object fromValue) {
    // toValue and fromValue are not null and not equal
    /* Possible cases:
     * fromValue=val toValue=val     ==> toValue=SET_VALUE  toValue__SET=Set<?>
     * fromValue=Set<?> toValue=val  ==> toValue=SET_VALUE  toValue__SET=Set<?>
     * fromValue=val toValue=Set<?>  ==>                    toValue__SET=Set<?>
     * fromValue=Set<?> toValue=Set<?>  ==>                 toValue__SET=Set<?>
     */

    // check special SET_SUFFIX property and create a Set if needed
    String setPropertyKey = key + SET_SUFFIX;
    String valueSetStr = toE.getProperty(setPropertyKey);
    ValueList valueList;
    if (valueSetStr == null) {
      valueList = new ValueList(false);
      Object existingVal = toE.getProperty(key);
      valueList.add(existingVal);
      toE.setProperty(key, SET_VALUE);
    } else {
      valueList = (ValueList) mapper.parser().parse(valueSetStr);
    }
    
    /// check if fromValue is already in toValueList
    boolean toValueChanged=false;
    if (fromValue.equals(SET_VALUE)) {
      String fromListStr=fromE.getProperty(setPropertyKey);
      ValueList fromValueList = (ValueList) mapper.parser().parse(fromListStr);
      for(Object fVal:fromValueList){
        if(!valueList.contains(fVal)){
          valueList.add(fVal); // hopefully, fromValue is the same type as other elements in the set
          toValueChanged=true;
        }
      }
    } else {
      if(!valueList.contains(fromValue)){
        valueList.add(fromValue); // hopefully, fromValue is the same type as other elements in the set
        toValueChanged=true;
      }
    }
    
    if(toValueChanged)
      toE.setProperty(setPropertyKey, mapper.toJson(valueList));
  }

  public static void main(String[] args) {
    ObjectMapper mapper = JsonFactory.create();
    //    String jsonArray = "[0,1,2,3,4,5,6,7,8,7]";
    //    Integer[] intArray = mapper.parser().parseIntArray( jsonArray );

    {
      String json = mapper.toJson("Hi");
      System.out.println(json);
      System.out.println(mapper.parser().parse(json).getClass());
    }
    {
      String json = mapper.toJson(1);
      System.out.println(json);
      System.out.println(mapper.parser().parse(json).getClass());
    }
    {
      HashSet<Integer> valueSet = new HashSet<>();
      valueSet.add(1);
      valueSet.add(2);
      valueSet.add(1);
      String intArrJson = mapper.toJson(valueSet);
      System.out.println(intArrJson);
      System.out.println(mapper.parser().parse(intArrJson).getClass());
    }

    {
      HashSet<String> valueSet = new HashSet<>();
      valueSet.add("a");
      valueSet.add("2");
      valueSet.add("b");
      valueSet.add("2");
      String inJson = mapper.toJson(valueSet);
      System.out.println(inJson);
      ValueList list = (ValueList) mapper.parser().parse(inJson);
      if (!list.contains("c"))
        list.add("c");
      if (!list.contains("c"))
        list.add("d");
      System.out.println(list);
    }
    {
      HashSet<Date> valueSet = new HashSet<>();
      valueSet.add(new Date());
      valueSet.add(new Date());
      String inJson = mapper.toJson(valueSet);
      System.out.println(inJson);
      ValueList list = (ValueList) mapper.parser().parse(inJson);
      System.out.println(list.get(0).getClass());
    }
    {
      HashSet<Object> valueSet = new HashSet<>();
      valueSet.add(new JsonPropertyMerger());
      String inJson = mapper.toJson(valueSet);
      System.out.println(inJson);
      ValueList list = (ValueList) mapper.parser().parse(inJson);
      System.out.println(list.get(0).getClass());
    }
  }

}
