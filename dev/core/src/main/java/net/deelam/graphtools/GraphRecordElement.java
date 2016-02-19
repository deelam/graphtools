package net.deelam.graphtools;

import java.io.Serializable;
import java.util.Map;

import com.tinkerpop.blueprints.Element;



public interface GraphRecordElement extends Element, Serializable {

  String getStringId();

  boolean equals(Object obj);

  Map<String, Object> getProps();
  
  void clearProperties();

}
