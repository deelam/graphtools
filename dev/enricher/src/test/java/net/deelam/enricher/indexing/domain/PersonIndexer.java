package net.deelam.enricher.indexing.domain;

import lombok.RequiredArgsConstructor;
import net.deelam.enricher.indexing.EntityIndexer;
import net.deelam.graphtools.GraphRecordImpl;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.BytesRef;

import com.tinkerpop.blueprints.Vertex;


public class PersonIndexer extends EntityIndexer {
  public static final String ENTITY_TYPE = "PERSON";
  
  @Override
  protected boolean isIndexable(Vertex v) {
    return GraphRecordImpl.getType(v).equals(ENTITY_TYPE);
  }
  
  @Override
  public void doIndex(Vertex v, Document doc) {
    Person p = new Person(v);
    // use a string field if we don't want it tokenized
    doc.add(new StringField("type", ENTITY_TYPE, Field.Store.YES));
    doc.add(new StringField("firstName", p.firstName(), Field.Store.YES));
    doc.add(new SortedDocValuesField("firstNameSorted", new BytesRef(p.firstName())));
    doc.add(new StringField("lastName", p.lastName(), Field.Store.YES));
    doc.add(new SortedDocValuesField("lastNameSorted", new BytesRef(p.lastName())));
  }
  
  @RequiredArgsConstructor
  static class Person{  
    final Vertex node;
  
    public String firstName() {
      return node.getProperty("firstName");
    }
  
    public String lastName() {
      return node.getProperty("lastName");
    }
  }
}
