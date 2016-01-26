/**
 * 
 */
package net.deelam.graphtools.graphfactories;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.IdGraphFactory;
import net.deelam.graphtools.JavaSetPropertyMerger;
import net.deelam.graphtools.PropertyMerger;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;

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
  public IdGraph<OrientGraph> open(GraphUri gUri) {
    Configuration conf=gUri.getConfig();
    /// copy properties to new keys that OrientGraph looks for
    String[] keys = Iterators.toArray(conf.getKeys(), String.class);
    for (String key : keys) {
      if (key.startsWith(CONFIG_PREFIX))
        continue;
      conf.setProperty(CONFIG_PREFIX + key, conf.getProperty(key));
    }
    //		GraphUri.printConfig(conf);
    setDefaultAuthentication(conf);

    URI uri = gUri.getUri();
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
  
  public void shutdown(GraphUri gUri, IdGraph<?> graph){
    OrientGraph oGraph=(OrientGraph) graph.getBaseGraph();
    oGraph.getRawGraph().getStorage().close(true, false); // hotfix: https://github.com/orientechnologies/orientdb/issues/5317#event-467569228
    graph.shutdown();
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
  
  enum DB_TYPE { plocal, memory, remote }

  private DB_TYPE getDBType(GraphUri gUri) {
    URI uri = gUri.getUri();
    return DB_TYPE.valueOf(uri.getScheme());
  }

  @Override
  public void delete(GraphUri gUri) throws IOException {
    DB_TYPE dbType = getDBType(gUri);
    if(dbType==DB_TYPE.plocal){
      File pathFile = new File(gUri.getUriPath());
      log.info("Deleting OrientDB at {}",pathFile);
      FileUtils.deleteDirectory(pathFile);
    }
  }

  @Override
  public void backup(GraphUri srcGraphUri, GraphUri dstGraphUri) throws IOException {
    File srcFile = new File(srcGraphUri.getUriPath());
    File destFile = new File(dstGraphUri.getUriPath());
    FileUtils.copyDirectory(srcFile, destFile);
  }
  
  @Override
  public boolean exists(GraphUri gUri) {
    DB_TYPE dbType = getDBType(gUri);
    if(dbType==DB_TYPE.plocal){
      File pathFile = new File(gUri.getUriPath());
      return pathFile.exists();
    }else{
      return false; // TODO: check remote
    }
  }

  @Override
  public PropertyMerger createPropertyMerger() {
    return new JavaSetPropertyMerger();
  }
}
