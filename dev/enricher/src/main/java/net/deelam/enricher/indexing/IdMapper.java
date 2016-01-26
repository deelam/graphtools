/**
 * 
 */
package net.deelam.enricher.indexing;

import java.io.*;
import java.util.Map;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
@NoArgsConstructor
@Slf4j
public class IdMapper implements AutoCloseable {

  @Getter
  String filename = null;
  final BidiMap map = new DualHashBidiMap();

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
  public void close() throws IOException {
    save();
  }

  private static ObjectMapper jsonObjMapper = JsonFactory.create();
  private static final String COUNTER_KEY = "__COUNTER__";

  private void loadMapFile() throws FileNotFoundException, IOException {
    if (new File(filename).exists())
      try (FileReader reader = new FileReader(filename)) {
        Map<?, ?> jsonMap = jsonObjMapper.parser().parse(Map.class, reader);
        map.putAll(jsonMap);
        Integer jsonCounter = (Integer) map.remove(COUNTER_KEY);
        counter = jsonCounter.intValue();
      }
  }

  public static IdMapper newFromFile(String filename) throws FileNotFoundException, IOException {
    IdMapper idMapper = new IdMapper();
    if (new File(filename).exists())
      try (FileReader reader = new FileReader(filename)) {
        Map<?, ?> jsonMap = jsonObjMapper.parser().parse(Map.class, reader);

        idMapper.map.putAll(jsonMap);
        Integer jsonCounter = (Integer) idMapper.map.remove(COUNTER_KEY);
        idMapper.counter = jsonCounter.intValue();

        idMapper.filename = filename;
        return idMapper;
      }
    else{
      log.debug("Creating new IdMapper using file={}", filename);
      idMapper.filename = filename;
      return idMapper;
    }
  }

  public static IdMapper newFromJson(String jsonStr) throws IOException {
    try (Reader reader = new StringReader(jsonStr)) {
      IdMapper idMapper = new IdMapper();
      Map<?, ?> jsonMap = jsonObjMapper.parser().parse(Map.class, reader);
      idMapper.map.putAll(jsonMap);
      Integer jsonCounter = (Integer) idMapper.map.remove(COUNTER_KEY);
      idMapper.counter = jsonCounter.intValue();
      return idMapper;
    }
  }

  public String toJson() {
    map.put(COUNTER_KEY, counter);
    String jsonStr = jsonObjMapper.toJson(map);
    map.remove(COUNTER_KEY);
    return jsonStr;
  }

  private void save() throws IOException {
    String jsonMap = toJson();
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
