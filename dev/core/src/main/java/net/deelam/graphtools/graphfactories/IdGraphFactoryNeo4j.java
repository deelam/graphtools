/**
 * 
 */
package net.deelam.graphtools.graphfactories;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.neo4j.backup.OnlineBackupSettings;

import com.google.common.collect.Iterators;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.IdGraphFactory;
import net.deelam.graphtools.JsonPropertyMerger;
import net.deelam.graphtools.Neo4jPropertyMerger;
import net.deelam.graphtools.PropertyMerger;

/**
 * @author deelam
 * 
 */
@Slf4j
public class IdGraphFactoryNeo4j implements IdGraphFactory {

  @Override
  public String getScheme() {
    return "neo4j";
  }
  
  private static final String CONFIG_PREFIX = "blueprints.neo4j.conf.";
  protected static final String BLUEPRINTS_NEO4J_DIRECTORY = "blueprints.neo4j.directory";

  public static void register() {
    GraphUri.register(new IdGraphFactoryNeo4j());
  }
  
  // Pass this to GraphUri on graphs that were built using BatchInserter.  
  // Ideally, this should be persisted with the graphstore but don't know how. 
  // http://stackoverflow.com/questions/21368277/neo4j-automatic-indexing-on-batch-execution
  // http://blog.armbruster-it.de/2013/12/indexing-in-neo4j-an-overview/
  private static Map<String,String> OPEN_AFTER_BATCH_INSERT_CONFIG=new HashMap<>();
  static {
    OPEN_AFTER_BATCH_INSERT_CONFIG.put("node_keys_indexable", IdGraph.ID );
    OPEN_AFTER_BATCH_INSERT_CONFIG.put("node_auto_indexing", "true" );
    OPEN_AFTER_BATCH_INSERT_CONFIG.put("relationship_keys_indexable", IdGraph.ID );
    OPEN_AFTER_BATCH_INSERT_CONFIG.put("relationship_auto_indexing", "true" );
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends KeyIndexableGraph> IdGraph<T> open(GraphUri gUri) {
    Configuration conf = gUri.getConfig(); //new BaseConfiguration();
    
    // avoids ConcurrentModificationException
    // cast is needed to prevent "Inconsistent stackmap frames" problem
    String[] keys = (String[]) Iterators.toArray(gUri.getConfig().getKeys(), String.class); 
    
    if(exists(gUri)){// assume graph has indexes supporting IdGraph, so they are not recreated
      synchronized(OPEN_AFTER_BATCH_INSERT_CONFIG){ // otherwise throws ConcurrentModificationException
        for (Entry<String, String> e:OPEN_AFTER_BATCH_INSERT_CONFIG.entrySet()) {
          String key = e.getKey();
          if(!conf.containsKey(CONFIG_PREFIX + key))
            conf.setProperty(CONFIG_PREFIX + key, e.getValue());
        }
      }
    }
    
    /// copy properties to new keys that Neo4jGraph looks for
    for (String key : keys) {
      if (key.startsWith("blueprints.neo4j"))
        continue;
      String val = gUri.getConfig().getString(key);
      if(!conf.containsKey(CONFIG_PREFIX + key))
        conf.setProperty(CONFIG_PREFIX + key, val);
    }
    
    String path = gUri.getUriPath();
    // open graph
    checkPath(path);

    log.debug("Opening Neo4j graph at path={}", path);
    // Set other settings using prefix "blueprints.neo4j.conf"
    String existingPath = conf.getString(BLUEPRINTS_NEO4J_DIRECTORY);
    if(existingPath==null){
      conf.setProperty(BLUEPRINTS_NEO4J_DIRECTORY, path);
    }else if(!path.equals(existingPath)){
      log.warn("Overwriting original path: {} with {}", conf.getProperty(BLUEPRINTS_NEO4J_DIRECTORY), path);
    }
    
    if(!conf.containsKey(CONFIG_PREFIX+OnlineBackupSettings.online_backup_enabled.name()))
      conf.setProperty(CONFIG_PREFIX+OnlineBackupSettings.online_backup_enabled.name(), "false");// if true, limits number of opened Neo4j graphs 
    
    //GraphUri.printConfig(conf);
    
    return (IdGraph<T>) openNeo4jGraph(gUri);
  }

  protected IdGraph<?> openNeo4jGraph(GraphUri gUri) {
    long waitTime=0;
    while(true){
      try{
        if(waitTime>0)
          log.info("Trying to open Neo4j graph at path={}",  gUri.getConfig().getProperty(BLUEPRINTS_NEO4J_DIRECTORY));
        IdGraph<Neo4jGraph> graph = new IdGraph<>(new Neo4jGraph(gUri.getConfig()));
        if(waitTime>0)
          log.info("Done waiting time={}; opened {}", waitTime, graph);
        return graph;
      }catch(RuntimeException re){
        if(re.getCause().getCause() instanceof org.neo4j.kernel.lifecycle.LifecycleException){
          log.warn("Graph already opened; waiting for it to close: {}", gUri);
          try {
            Thread.sleep(5000); // TODO: 4: sleeping may be unfair if some other Thread opens it before this wakes up.  Queue would be fair but only works within a single JVM.  
            waitTime+=5;
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }else{
          log.error("Exception cause={}", re.getCause().getCause().getClass());
          throw re;
        }
      }
    }
  }
  
  public static void main1(String[] args) throws InterruptedException {
    IdGraphFactoryNeo4j.register();
    
    new Thread(new Runnable(){
      public  void run() {
        log.info("Started 1");
        GraphUri guri = new GraphUri("neo4j:neoOneAtATime");
        guri.openIdGraph();
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        guri.shutdown();
      }
    }).start();
    
    
    new Thread(new Runnable(){
      public  void run() {
        log.info("Started 2");
        GraphUri guri = new GraphUri("neo4j:neoOneAtATime");
        guri.openIdGraph();
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        guri.shutdown();
      }
    }).start();
    
  }
  
  protected void checkPath(String path) {
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
    return new Neo4jPropertyMerger();
  }
  
}
