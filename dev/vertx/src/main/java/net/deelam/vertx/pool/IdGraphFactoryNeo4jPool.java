/**
 * 
 */
package net.deelam.vertx.pool;

import java.io.IOException;

import org.boon.collections.MultiMap;
import org.boon.collections.MultiMapImpl;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.IdGraphFactory;
import net.deelam.graphtools.ReadOnlyIdGraph;
import net.deelam.graphtools.graphfactories.IdGraphFactoryNeo4j;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;

/**
 * @author deelam
 * 
 */
@Slf4j
public class IdGraphFactoryNeo4jPool extends IdGraphFactoryNeo4j {

  private final ResourcePoolClient poolClient;
  
  @Deprecated
  public static void register() {
    throw new UnsupportedOperationException("Use register(Injector injector) instead!"); 
  }
  
  private boolean isReadOnly(GraphUri gUri) {
    return gUri.getConfig().getBoolean(IdGraphFactory.READONLY, false);
  }
  
  @Override
  public void delete(GraphUri gUri) throws IOException {
    if(isReadOnly(gUri)){
      log.warn("Not deleting since this is a read-only copy of the original graph: {}", gUri);
    } else {
      super.delete(gUri);
    }
  }

  @Override
  public void backup(GraphUri srcGraphUri, GraphUri dstGraphUri) throws IOException {
    if(isReadOnly(srcGraphUri)){
      log.error("Not backing up since this is a read-only copy of the original graph: {}", srcGraphUri);
    } else {
      super.backup(srcGraphUri, dstGraphUri);
    }
  }
  
  @Override
  public boolean exists(GraphUri gUri) {
    if(super.exists(gUri))
      return true;
    
    if(isReadOnly(gUri)){
      log.error("Graph may exists on another machine; assuming graph exists: {}", gUri);
      return true;
    } else {
      return false;
    }
  }
  
  public static void register(Injector injector) {
    GraphUri.register(injector.getInstance(IdGraphFactoryNeo4jPool.class));
  }

  @Inject
  public IdGraphFactoryNeo4jPool(ResourcePoolClient client) {
    super();
    poolClient=client;
    poolClient.setResourceConsumer(msg->{
      String origUri=msg.headers().get(PondVerticle.ORIGINAL_URI);
      GraphUri gUri = null;
      synchronized(waitingGraphUris){
        gUri = waitingGraphUris.getFirst(origUri);
        if(gUri==null){
          log.error("Cannot find {} in {}", origUri, waitingGraphUris.baseMap());
        }else{
          if(!waitingGraphUris.removeValueFrom(origUri, gUri))
            log.warn("Could not remove {} from {}", gUri, waitingGraphUris.baseMap());
          if(waitingGraphUris.getFirst(origUri)==null){
            log.info("Removing empty key={} from {}", origUri, waitingGraphUris.baseMap());
            waitingGraphUris.remove(origUri);
          }
        }
      }
      if(gUri!=null) synchronized(gUri){
          String uri = msg.body();
          String path=new GraphUri(uri).getUriPath();
          checkPath(path);

          log.info("Got copy of Neo4j graph at path={}", path);
          // Set other settings using prefix "blueprints.neo4j.conf"
          gUri.getConfig().setProperty(BLUEPRINTS_NEO4J_DIRECTORY, path);
          gUri.getConfig().setProperty(POOL_RESOURCE_URI, uri);
          gUri.notify();
      }
    });
  }
  
  private MultiMap<String,GraphUri> waitingGraphUris=new MultiMapImpl<>();
  
  public static void main(String[] args) {
    IdGraphFactoryTinker.register();
    MultiMap<String,GraphUri> mm=new MultiMapImpl<>();
    mm.put("a", new GraphUri("tinker:/"));
    mm.put("a", new GraphUri("tinker:///"));
    System.out.println(mm.baseMap());
    GraphUri first = mm.getFirst("a");
    System.out.println(new GraphUri("tinker:/").equals(first)+" "+first);
    mm.removeValueFrom("a", first);
    System.out.println(mm.baseMap());
    
    mm.put("a", new GraphUri("tinker:///asdf"));
    GraphUri next = mm.getFirst("a");
    System.out.println((new GraphUri("tinker:///").equals(next))+" "+next);
    mm.removeValueFrom("a", next);
    System.out.println(mm.baseMap());
    
    GraphUri last = mm.getFirst("a");
    mm.removeValueFrom("a", last);
    System.out.println(mm.baseMap());
    System.out.println(mm.getFirst("a")==null);
    if(mm.getFirst("a")==null)
      mm.remove("a");
    System.out.println(mm.baseMap().size()==0);
  }
  
  @Override
  protected IdGraph<?> openNeo4jGraph(GraphUri gUri) {
    if(isReadOnly(gUri)){
      gUri.getConfig().clearProperty(BLUEPRINTS_NEO4J_DIRECTORY); // to ensure we don't open original graph
      synchronized(waitingGraphUris){
        log.info("Putting {} into waitingGraphUris={}", gUri.asString(), waitingGraphUris.baseMap());
        waitingGraphUris.put(gUri.asString(), gUri);
      }
      synchronized(gUri){
        poolClient.checkout(gUri.asString()); // async; can finish at any time
        try {
          log.info("Waiting for resource pool to get graph: "+gUri);
          gUri.wait(); // wait for response
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      log.debug("Opening read-only copy of Neo4j graph at new path={}", gUri);
      ReadOnlyIdGraph graph = new ReadOnlyIdGraph(new Neo4jGraph(gUri.getConfig()));
//      IdGraph<?> graph = new IdGraph<>(new MyReadOnlyKeyIndexableGraph<>(new IdGraph<>((new Neo4jGraph(gUri.getConfig())))));
      log.info("  Opened read-only graph={}", graph); 
      return graph;
    } else {
      return super.openNeo4jGraph(gUri);
    }
  }

  private static final String POOL_RESOURCE_URI = "poolResourceUri";
  
  @Override
  public void shutdown(GraphUri gUri, IdGraph<?> graph){
    graph.shutdown();
    
    if(isReadOnly(gUri)){
      String copiedRsrcUri = gUri.getConfig().getString(POOL_RESOURCE_URI);
      log.info("Returning pool resource: {}", copiedRsrcUri);
      poolClient.checkin(copiedRsrcUri);
    } else {
      log.info("Adding pool resource: {}", gUri.asString());
      poolClient.add(gUri.asString());
    }
  }
  
}
