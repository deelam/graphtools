/**
 * 
 */
package net.deelam.graphtools;

import java.util.HashMap;
import java.util.Map;

import com.tinkerpop.blueprints.Parameter;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * @author dnlam
 *
 */
@Slf4j
public final class GraphIndexConstants {

  private static final Map<String, PropertyKeys> propertyKeysMap = new HashMap<>();

  public static PropertyKeys put(String key, PropertyKeys pks) {
    return propertyKeysMap.put(key, pks);
  }

  public static PropertyKeys getPropertyKeys(String key) {
    PropertyKeys pks = propertyKeysMap.get(key);
    if(pks==null)
      log.error("Could not find PropertyKeys for {}", key);
    return pks;
  }

  /// parameter names
  public static final String DATA_CLASS = "dataClass";
  public static final String CARDINALITY = "cardinality";

  @Data
  public static final class PropertyKeys {
    final Map<String, Parameter[]> vertexKeys = new HashMap<>();
    final Map<String, Parameter[]> edgeKeys = new HashMap<>();
  }

}
