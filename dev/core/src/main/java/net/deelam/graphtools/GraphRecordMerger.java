package net.deelam.graphtools;

import com.tinkerpop.blueprints.Element;


public interface GraphRecordMerger {

  void merge(GraphRecord from, GraphRecord to);

  void mergeProperties(Element fromE, Element toE);

}
