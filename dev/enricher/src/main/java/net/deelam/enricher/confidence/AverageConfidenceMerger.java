package net.deelam.enricher.confidence;

import com.tinkerpop.blueprints.Element;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.enricher.confidence.ConfidenceGraph.ConfidenceMerger;

@RequiredArgsConstructor
@Slf4j 
public class AverageConfidenceMerger implements ConfidenceMerger {
  private static final String CONFIDENCE_WEIGHT_SUFFIX = ConfidenceGraph.CONFIDENCE_SUFFIX+"Weight";
  
  private final ConfidenceGraph cgraph;
  
  @Override
  public void merge(Element fromElem, Element toElem, String propName) {
    Integer fromConf = cgraph.getConfidence(fromElem, propName);
    Integer toConf = cgraph.getConfidence(toElem, propName);
    
    Integer fromWeight = getConfidenceWeight(fromElem, propName);
    Integer toWeight = getConfidenceWeight(toElem, propName);
    
    if(! fromElem.getProperty(propName).equals(toElem.getProperty(propName))){
      // TODO: 4: handle multiple different values when merging confidences 
      log.warn("Property values are not equal: {} != {}", fromElem.getProperty(propName), toElem.getProperty(propName));
    }
    
    int totalWeight=toWeight+fromWeight;
    int avConf = ((toConf*toWeight) + (fromConf*fromWeight)) / totalWeight;
    cgraph.setConfidence(toElem, propName, avConf);
    
    toElem.setProperty(propName+CONFIDENCE_WEIGHT_SUFFIX, totalWeight);
  }

  private Integer getConfidenceWeight(Element toElem, String propName) {
    Integer weight = toElem.getProperty(propName+CONFIDENCE_WEIGHT_SUFFIX);
    if(weight==null)
      weight=1;
    return weight;
  }
}