package net.deelam.enricher.confidence;

import com.tinkerpop.blueprints.Element;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.enricher.confidence.ConfidenceGraph.ConfidenceMerger;

@RequiredArgsConstructor
@Slf4j 
public class MaxConfidenceMerger implements ConfidenceMerger {
  private final ConfidenceGraph cgraph;
  
  @Override
  public void merge(Element fromElem, Element toElem, String propName) {
    Integer fromConf = cgraph.getConfidence(fromElem, propName);
    Integer toConf = cgraph.getConfidence(toElem, propName);
    
    if(! fromElem.getProperty(propName).equals(toElem.getProperty(propName))){
      // TODO 4: handle multiple different values when merging confidences 
      log.warn("Property values are not equal: {} != {}", fromElem.getProperty(propName), toElem.getProperty(propName));
    }
    
    int conf = Math.max(toConf, fromConf);
    cgraph.setConfidence(toElem, propName, conf);
  }
}