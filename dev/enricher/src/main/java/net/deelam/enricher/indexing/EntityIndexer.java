/**
 * 
 */
package net.deelam.enricher.indexing;

import org.apache.lucene.document.Document;

import com.tinkerpop.blueprints.Vertex;

/**
 * @author deelam
 *
 */
public abstract class EntityIndexer {

  public Document index(Vertex v) {
    Document doc = new Document();
    index(v, doc);
    return doc;
  }

  abstract public void index(Vertex v, Document doc);

}
