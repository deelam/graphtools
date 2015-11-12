/**
 * 
 */
package net.deelam.graphtools.graphfactories;

import java.net.URI;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.IdGraphFactory;

import org.apache.commons.configuration.Configuration;

import com.google.common.collect.Iterators;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
//import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

/**
 * @author deelam
 * 
 */
@Slf4j
public class IdGraphFactoryOrientdb implements IdGraphFactory {

  private static final String CONFIG_PREFIX = "blueprints.orientdb.";

  public static void register() {
    GraphUri.register("orientdb", new IdGraphFactoryOrientdb());
  }

  @SuppressWarnings("unchecked")
  @Override
  public IdGraph<OrientGraph> open(Configuration conf) {

    /// copy properties to new keys that OrientGraph looks for
    String[] keys = Iterators.toArray(conf.getKeys(), String.class);
    for (String key : keys) {
      if (key.startsWith(CONFIG_PREFIX))
        continue;
      conf.setProperty(CONFIG_PREFIX + key, conf.getProperty(key));
    }
    //		GraphUri.printConfig(conf);
    setDefaultAuthentication(conf);

    URI uri = (URI) conf.getProperty(GraphUri.BASE_URI);
    // open graph
    IdGraph<OrientGraph> graph;
    if (uri == null) {
      throw new IllegalArgumentException("Provide a URI like so: plocal:/tmp/.., remote:localhost/tinkpop, memory:tinkpop");
    } else {
      log.info("Opening OrientDB graph at URI=" + uri);
      // Set other settings using prefix "blueprints.orientdb"
      conf.setProperty(CONFIG_PREFIX+"url", uri.toString());
      graph = new IdGraph<>(new OrientGraph(conf));
    }
    return graph;
  }

  private void setDefaultAuthentication(Configuration conf) {
    String username = conf.getString(CONFIG_PREFIX+"username");
    if(username==null){
      log.info("Using default OrientDB username");
      conf.setProperty(CONFIG_PREFIX+"username", "admin");
    }
    
    String pwd = conf.getString(CONFIG_PREFIX+"password");
    if(pwd==null){
      log.info("Using default OrientDB password");
      conf.setProperty(CONFIG_PREFIX+"password", "admin");
    }
  }

}
