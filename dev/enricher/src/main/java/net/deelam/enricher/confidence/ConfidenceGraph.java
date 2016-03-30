package net.deelam.enricher.confidence;

import static com.google.common.base.Preconditions.checkNotNull;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUtils;

/**
 * Tracking confidence of property values
 * 
 * A node or edge can have a confidence value for a particular property.  
 * Each node or edge type can have a default confidence value for a particular property.
 * Each node or edge type can have a default confidence value, relevant to all of its property values.
 * A graph (DataSource) can have a default confidence value, relevant to all of its property values.
 * 
 * Confidences change when merging nodes or edges.  2 case scenarios:
 * 1. When ingesting a data source, apply intra-datasource merge policy.
 * 2. When merging from multiple data sources, apply cross-datasource merge policy.
 * These assume the values of a property are equal.
 * 
 * If values of a property are not equal:
 *   A more complete solution would track the confidence of each property value (and their relationships).
 *   Eg. If value is boolean, then should maintain confidences such that: confidence of true = 1 - confidence of false
 *   Eg. If value is a range from 0 to 100, then a distribution of confidence for each value should be modeled.
 *     How is this distribution presented to the user?
 *   This detailed modeling of confidences can become overly complex and incurs computational and storage costs. 
 *   For now, do the same as if values were equal.
 *  
 * @author dd
 */
@RequiredArgsConstructor
@Slf4j
public class ConfidenceGraph {
  static final String CONFIDENCE_SUFFIX = "._conf";

  private final IdGraph<?> graph;

  private final String nodeTypePropertyKey;
  private final String edgeTypePropertyKey;

  //

  public void setDatasourceDefaultConfidence(int conf) {
    Vertex md = GraphUtils.getMetaDataNode(graph);
    String key = "datasource" + CONFIDENCE_SUFFIX;
    if (md.getProperty(key) != null)
      throw new IllegalStateException("Default confidene already set for " + key);
    md.setProperty(key, conf);
  }

  public Integer getDatasourceDefaultConfidence() {
    Vertex md = GraphUtils.getMetaDataNode(graph);
    String key = "datasource" + CONFIDENCE_SUFFIX;
    Integer conf = md.getProperty(key);
    if (conf == null) {
      log.warn("No datasource confidence set; using 50%.");
      return 50;
    }
    return conf;
  }

  ////

  public void setNodeDefaultConfidence(String nodeType, int conf) {
    Vertex md = GraphUtils.getMetaDataNode(graph);
    String key = "node." + nodeType + CONFIDENCE_SUFFIX;
    if (md.getProperty(key) != null)
      throw new IllegalStateException("Default confidene already set for " + key);
    md.setProperty(key, conf);
  }

  public Integer getNodeDefaultConfidence(Vertex elem) {
    Vertex md = GraphUtils.getMetaDataNode(graph);
    String nodeType = elem.getProperty(nodeTypePropertyKey);
    return md.getProperty("node." + nodeType + CONFIDENCE_SUFFIX);
  }

  //
  
  public void setEdgeDefaultConfidence(String edgeType, int conf) {
    Vertex md = GraphUtils.getMetaDataNode(graph);
    String key = "edge." + edgeType + CONFIDENCE_SUFFIX;
    if (md.getProperty(key) != null)
      throw new IllegalStateException("Default confidene already set for " + key);
    md.setProperty(key, conf);
  }

  public Integer getEdgeDefaultConfidence(Edge elem) {
    Vertex md = GraphUtils.getMetaDataNode(graph);
    String edgeType = elem.getProperty(edgeTypePropertyKey);
    return md.getProperty("edge." + edgeType + CONFIDENCE_SUFFIX);
  }

  ////

  public void setNodeDefaultConfidence(String nodeType, String propName, int conf) {
    Vertex md = GraphUtils.getMetaDataNode(graph);
    String key = "node." + nodeType + "." + propName + CONFIDENCE_SUFFIX;
    if (md.getProperty(key) != null)
      throw new IllegalStateException("Default confidene already set for " + key);
    md.setProperty(key, conf);
  }

  public Integer getNodeDefaultConfidence(Vertex elem, String propName) {
    Vertex md = GraphUtils.getMetaDataNode(graph);
    String nodeType = elem.getProperty(nodeTypePropertyKey);
    return md.getProperty("node." + nodeType + "." + propName + CONFIDENCE_SUFFIX);
  }

  //

  public void setEdgeDefaultConfidence(String edgeType, String propName, int conf) {
    Vertex md = GraphUtils.getMetaDataNode(graph);
    String key = "edge." + edgeType + "." + propName + CONFIDENCE_SUFFIX;
    if (md.getProperty(key) != null)
      throw new IllegalStateException("Default confidene already set for " + key);
    md.setProperty(key, conf);
  }

  public Integer getEdgeDefaultConfidence(Edge elem, String propName) {
    Vertex md = GraphUtils.getMetaDataNode(graph);
    String edgeType = elem.getProperty(edgeTypePropertyKey);
    return md.getProperty("edge." + edgeType + "." + propName + CONFIDENCE_SUFFIX);
  }

  ////

  // can be used in ingester as static method
  public static void setConfidence(Element elem, String propName, int conf) {
    elem.setProperty(propName + CONFIDENCE_SUFFIX, conf);
  }

  public Integer getConfidence(Element elem, String propName) {
    Integer conf = elem.getProperty(propName + CONFIDENCE_SUFFIX);
    if (conf != null)
      return conf;

    if (elem instanceof Vertex) {
      conf = getNodeDefaultConfidence((Vertex) elem, propName);
      if(conf==null)
        conf=getNodeDefaultConfidence((Vertex) elem);
    } else if (elem instanceof Edge) {
      conf = getEdgeDefaultConfidence((Edge) elem, propName);
      if(conf==null)
        conf=getEdgeDefaultConfidence((Edge) elem);
    } else {
      throw new IllegalArgumentException("Parameter should be a graph Element: " + elem.getClass());
    }
    if (conf != null)
      return conf;

    conf = getDatasourceDefaultConfidence();
    checkNotNull(conf, "Default confidence not set!");
    return conf;
  }

  ////

  static interface ConfidenceMerger {
    void merge(Element fromElem, Element toElem, String propName);
  }

  @Setter
  private ConfidenceMerger confMerger = new AverageConfidenceMerger(this);

  public void mergeConfidence(Element fromElem, Element toElem, String propName) {
    confMerger.merge(fromElem, toElem, propName);
  }

  ////

}
