/**
 * 
 */
package net.deelam.graphtools.graphfactories;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.IdGraphFactory;

import org.apache.commons.configuration.Configuration;

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
  public IdGraph<Neo4jGraph> open(Configuration conf) {

    /// copy properties to new keys that Neo4jGraph looks for
    String[] keys = Iterators.toArray(conf.getKeys(), String.class);
    for (String key : keys) {
      if (key.startsWith("blueprints.neo4j"))
        continue;
      conf.setProperty(CONFIG_PREFIX + key, conf.getProperty(key));
    }
    //		GraphUri.printConfig(conf);

    String path = conf.getString(GraphUri.URI_PATH);
    // open graph
    IdGraph<Neo4jGraph> graph;
    if (path == null || path.equals("/")) {
      throw new IllegalArgumentException("Provide a path like so: 'neo4j://./myDB'");
    } else {
      log.debug("Opening Neo4j graph at path=" + path);
      // Set other settings using prefix "blueprints.neo4j.conf"
      conf.setProperty("blueprints.neo4j.directory", path);
      graph = new IdGraph<>(new Neo4jGraph(conf));
    }
    return graph;
  }

}
