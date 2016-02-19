package net.deelam.enricher.indexing.domain;

import net.deelam.enricher.indexing.EntityIndexer;
import net.deelam.graphtools.GraphRecordImpl;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;

import com.tinkerpop.blueprints.Vertex;

public class LocationIndexer extends EntityIndexer {
  public static final String ENTITY_TYPE = "LOCATION";

  @Override
  protected boolean isIndexable(Vertex v) {
    return GraphRecordImpl.getType(v).equals(ENTITY_TYPE);
  }

  @Override
  public void doIndex(Vertex v, Document doc) {
    Location loc = new Location(v);
    doc.add(new StringField("type", ENTITY_TYPE, Field.Store.YES));
    switch (loc.locationType()) {
      case Company:
        if (loc.zip() != null){
          doc.add(new IntField("zip", loc.zip(), Field.Store.YES));
          doc.add(new SortedNumericDocValuesField("zipSorted", loc.zip().longValue()));
        }
        doc.add(new TextField("address", loc.address(), Field.Store.YES));
        break;
    }
  }

  static class Location {
    final Vertex node;

    public enum LOCATION_TYPES {
      Company, USState
    };

    public Location(Vertex node) {
      this.node = node;
    }

    public LOCATION_TYPES locationType() {
      String locType = node.getProperty("locationType");
      if (locType != null)
        return LOCATION_TYPES.valueOf(locType);
      return null;
    }

    public String name() {
      return node.getProperty("name");
    }

    public String address() {
      return node.getProperty("address");
    }

    public String city() {
      return node.getProperty("city");
    }

    public String county() {
      return node.getProperty("county");
    }

    public String state() {
      return node.getProperty("state");
    }

    public Integer zip() {
      return node.getProperty("zip");
    }
  }
}
