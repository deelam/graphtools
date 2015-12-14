/**
 * 
 */
package net.deelam.graphtools.graphfactories;

import java.io.File;
import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.IdGraphFactory;

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

  @SuppressWarnings("unchecked")
  @Override
  public IdGraph<Neo4jGraph> open(GraphUri gUri) {
    Configuration conf = gUri.getConfig();
    /// copy properties to new keys that Neo4jGraph looks for
    String[] keys = Iterators.toArray(conf.getKeys(), String.class);
    for (String key : keys) {
      if (key.startsWith("blueprints.neo4j"))
        continue;
      conf.setProperty(CONFIG_PREFIX + key, conf.getProperty(key));
    }
    //		GraphUri.printConfig(conf);

    String path = gUri.getUriPath();
    // open graph
    checkPath(path);

    log.debug("Opening Neo4j graph at path=" + path);
    // Set other settings using prefix "blueprints.neo4j.conf"
    conf.setProperty("blueprints.neo4j.directory", path);
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
      FileUtils.deleteDirectory(pathFile);
    }
  }

  @Override
  public boolean exists(GraphUri gUri) {
    File pathFile = new File(gUri.getUriPath());
    return pathFile.exists();
  }

}
