package net.deelam.graphtools.importer;

import com.tinkerpop.blueprints.Element;

/**
 * @author dnlam, Created:Dec 9, 2015
 */
public interface PropertyMerger {
// TODO: add unit tests for different cases and different classes (eg., Date)
  void mergeProperties(Element fromE, Element toE);

}
