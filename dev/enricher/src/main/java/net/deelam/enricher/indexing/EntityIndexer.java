/**
 * 
 */
package net.deelam.enricher.indexing;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

import com.tinkerpop.blueprints.Vertex;

/**
 * @author deelam
 *
 */
public abstract class EntityIndexer {

  public Document index(Vertex v, Document doc) {
    if(isIndexable(v)){
      if(doc==null)
        doc = new Document();
      doIndex(v, doc);
      return doc;
    }
    return null;
  }

  protected boolean isIndexable(Vertex v) {
    return true;
  }

  abstract protected void doIndex(Vertex v, Document doc);

  private static final Map<String, Analyzer> EMPTY_MAP = new HashMap<>(0);
  public Map<String, Analyzer> createAnalyzers(){
    return EMPTY_MAP;
  }

}
