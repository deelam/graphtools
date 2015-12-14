/**
 * 
 */
package net.deelam.enricher.indexing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.collections.bidimap.UnmodifiableBidiMap;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;

import com.google.common.base.Preconditions;

/**
 * @author dlam
 *
 */
public class IdMapper implements AutoCloseable {

  final String filename;
  final BidiMap map = new DualHashBidiMap();

  public IdMapper(String filename) throws FileNotFoundException, IOException {
    this.filename = filename;
    load();
  }

  public void put(String shortStrId, String longStrId) {
    map.put(shortStrId, longStrId);
  }

  public String getShortIdIfExists(String longStrId) {
    return (String) map.getKey(longStrId);
  }

  public String shortId(String longStrId) {
    String sName = getShortIdIfExists(longStrId);
    if (sName == null) {
      sName = genNewId();
      put(sName, longStrId);
    }
    return sName;
  }

  int counter = 0;

  private String genNewId() {
    String newId;
    do {
      newId = "_" + (++counter);
    } while (map.containsKey(newId));
    return newId;
  }

  public String longId(String shortStrId) {
    return (String) map.get(shortStrId);
  }

  public BidiMap getMap() {
    return UnmodifiableBidiMap.decorate(map);
  }

  @Override
  public void close() throws Exception {
    save();
  }

  private ObjectMapper mapper = JsonFactory.create();
  private static final String COUNTER_KEY = "__COUNTER__";

  private void load() throws FileNotFoundException, IOException {
    if (new File(filename).exists())
      try (FileReader reader = new FileReader(filename)) {
        Map<?, ?> jsonMap = mapper.parser().parse(Map.class, reader);
        map.putAll(jsonMap);
        Integer jsonCounter = (Integer) map.remove(COUNTER_KEY);
        counter = jsonCounter.intValue();
      }
  }

  private void save() throws IOException {
    map.put(COUNTER_KEY, counter);
    String jsonMap = mapper.toJson(map);
    map.remove(COUNTER_KEY);
    try (FileWriter writer = new FileWriter(filename, false)) {
      writer.write(jsonMap);
    }
  }

  public void replaceLongId(String shortStrId, String longStrId) {
    Preconditions.checkNotNull(map.get(shortStrId), "ShortId doesn't exist: " + shortStrId);
    put(shortStrId, longStrId);
  }

  public void replaceShortId(String shortStrId, String longStrId) {
    Preconditions.checkNotNull(map.getKey(longStrId), "LongId doesn't exist: " + longStrId);
    put(shortStrId, longStrId);
  }
}
