/**
 * 
 */
package net.deelam.graphtools.graphfactories;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.IdGraphFactory;
import net.deelam.graphtools.JsonPropertyMerger;
import net.deelam.graphtools.PropertyMerger;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;

import com.google.common.collect.Iterators;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 * 
 */
@Slf4j
public class IdGraphFactoryNeo4j implements IdGraphFactory {

  private static final String CONFIG_PREFIX = "blueprints.neo4j.conf.";

  public static void register() {
    GraphUri.register("neo4j", new IdGraphFactoryNeo4j());
  }
  
  // Pass this to GraphUri on graphs that were built using BatchInserter.  
  // Ideally, this should be persisted with the graphstore but don't know how. 
  // http://stackoverflow.com/questions/21368277/neo4j-automatic-indexing-on-batch-execution
  // http://blog.armbruster-it.de/2013/12/indexing-in-neo4j-an-overview/
  public static Configuration OPEN_AFTER_BATCH_INSERT_CONFIG=new BaseConfiguration();
  static {
    OPEN_AFTER_BATCH_INSERT_CONFIG.setProperty("node_keys_indexable", IdGraph.ID );
    OPEN_AFTER_BATCH_INSERT_CONFIG.setProperty("node_auto_indexing", "true" );
    OPEN_AFTER_BATCH_INSERT_CONFIG.setProperty("relationship_keys_indexable", IdGraph.ID );
    OPEN_AFTER_BATCH_INSERT_CONFIG.setProperty("relationship_auto_indexing", "true" );
  }

  @SuppressWarnings("unchecked")
  @Override
  public IdGraph<Neo4jGraph> open(GraphUri gUri) {
    CompositeConfiguration conf = new CompositeConfiguration();
    if(exists(gUri)){// assume graph has indexes supporting IdGraph, so they are not recreated
      for (Iterator<String> itr = OPEN_AFTER_BATCH_INSERT_CONFIG.getKeys(); itr.hasNext();) {
        String key = itr.next();
        conf.setProperty(CONFIG_PREFIX + key, OPEN_AFTER_BATCH_INSERT_CONFIG.getProperty(key));
      }
    }
    
    conf.addConfiguration(gUri.getConfig());
    /// copy properties to new keys that Neo4jGraph looks for
    String[] keys = Iterators.toArray(gUri.getConfig().getKeys(), String.class); // avoids ConcurrentModificationException
    for (String key : keys) {
      if (key.startsWith("blueprints.neo4j"))
        continue;
      conf.setProperty(CONFIG_PREFIX + key, gUri.getConfig().getProperty(key));
    }
    
    String path = gUri.getUriPath();
    // open graph
    checkPath(path);

    log.debug("Opening Neo4j graph at path={}", path);
    // Set other settings using prefix "blueprints.neo4j.conf"
    conf.setProperty("blueprints.neo4j.directory", path);
//    GraphUri.printConfig(conf);
    IdGraph<Neo4jGraph> graph = new IdGraph<>(new Neo4jGraph(conf));
    return graph;
  }

  public void shutdown(GraphUri gUri, IdGraph<?> graph){
    graph.shutdown();
  }
  
  private void checkPath(String path) {
    if (path == null || path.equals("/")) {
      throw new IllegalArgumentException("Provide a path like so: 'neo4j://./myDB'");
    }
  }

  @Override
  public void delete(GraphUri gUri) throws IOException {
    File pathFile = new File(gUri.getUriPath());
    if(pathFile.exists()){
      log.info("Deleting Neo4j DB at {}",pathFile);
      System.gc(); // addresses problem with NFS files still being held by JVM 
      FileUtils.deleteDirectory(pathFile);
    }
  }

  @Override
  public boolean exists(GraphUri gUri) {
    File pathFile = new File(gUri.getUriPath());
    return pathFile.exists();
  }


  @Override
  public void backup(GraphUri srcGraphUri, GraphUri dstGraphUri) throws IOException {
    File srcFile = new File(srcGraphUri.getUriPath());
    File destFile = new File(dstGraphUri.getUriPath());
    FileUtils.copyDirectory(srcFile, destFile);
  }

  @Override
  public PropertyMerger createPropertyMerger() {
    return new JsonPropertyMerger();
  }
}
