/**
 * 
 */
package net.deelam.enricher.indexing;

import static com.google.common.base.Preconditions.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.collections.bidimap.UnmodifiableBidiMap;
import org.apache.commons.io.output.FileWriterWithEncoding;
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
  
  public IdMapper copyAs(String newFilename){
    IdMapper newMapper=copy();
    newMapper.filename=newFilename;
    return newMapper;
  }
  
  public IdMapper copy(){
    IdMapper newMapper=new IdMapper();
    newMapper.counter=counter;
    newMapper.map.putAll(map);
    return newMapper;
  }
  
  @Getter
  String filename = null;
  private final BidiMap map = new DualHashBidiMap();
  
  @SuppressWarnings("unchecked")
  public Set<String> getShortIdSet(){
    return map.keySet();
  }

  public void put(String shortStrId, String longStrId) {
    checkNotNull(longStrId, "longId cannot be null"/*+shortStrId*/);
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

  private static final String GRAPH_ID_PREFIX = "_";
  int counter = 0;

  private String genNewId() {
    String newId;
    do {
      newId = GRAPH_ID_PREFIX + (++counter);
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
    if(filename!=null)
      saveToFile();
  }

  private static ObjectMapper jsonObjMapper = JsonFactory.create();
  private static final String COUNTER_KEY = "__COUNTER__";

/*  private void loadMapFile() throws FileNotFoundException, IOException {
    if (new File(filename).exists())
      try (FileReader reader = new FileReader(filename)) {
        Map<?, ?> jsonMap = jsonObjMapper.parser().parse(Map.class, reader);
        map.putAll(jsonMap);
        Integer jsonCounter = (Integer) map.remove(COUNTER_KEY);
        counter = jsonCounter.intValue();
      }
  }*/

  @SuppressWarnings("unchecked")
  public static IdMapper newFromFile(String filename) throws FileNotFoundException, IOException {
    IdMapper idMapper = new IdMapper();
    if (new File(filename).exists())
      try (Reader reader = new InputStreamReader(new FileInputStream(filename), StandardCharsets.UTF_8)) {
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

/*  @SuppressWarnings("unchecked")
  public static IdMapper newFromJson(String jsonStr) throws IOException {
    try (Reader reader = new StringReader(jsonStr)) {
      IdMapper idMapper = new IdMapper();
      Map<?, ?> jsonMap = jsonObjMapper.parser().parse(Map.class, reader);
      idMapper.map.putAll(jsonMap);
      Integer jsonCounter = (Integer) idMapper.map.remove(COUNTER_KEY);
      idMapper.counter = jsonCounter.intValue();
      return idMapper;
    }
  }*/

  public String toJson() {
    map.put(COUNTER_KEY, counter);
    String jsonStr = jsonObjMapper.toJson(map);
    map.remove(COUNTER_KEY);
    return jsonStr;
  }

  private void saveToFile() throws IOException {
    String jsonMap = toJson();
    try (Writer writer = new FileWriterWithEncoding(filename, StandardCharsets.UTF_8)) {
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
