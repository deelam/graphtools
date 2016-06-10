package net.deelam.graphtools;

import java.util.List;

import com.tinkerpop.blueprints.Element;

/**
 * @author dnlam, Created:Dec 9, 2015
 */
public interface PropertyMerger {
  static final String VALUELIST_SUFFIX = "$L";

  // TODO: add unit tests for different cases and different classes (eg., Date)
  void mergeProperties(Element fromE, Element toE);
  
  <T> List<T> getListProperty(Element elem, String key);
  
  int getListPropertySize(Element elem, String key);

  boolean addProperty(Element elem, String key, Object value);

  boolean isMultivalued(Object value);
  
}
