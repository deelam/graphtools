/**
 * 
 */
package net.deelam.vertx.pool;

import java.io.IOException;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.IdGraphFactory;
import net.deelam.graphtools.ReadOnlyIdGraph;
import net.deelam.graphtools.graphfactories.IdGraphFactoryNeo4j;

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
  }
  
  @Override
  protected IdGraph<?> openNeo4jGraph(GraphUri gUri) {
    if(isReadOnly(gUri)){
      gUri.getConfig().clearProperty(BLUEPRINTS_NEO4J_DIRECTORY); // to ensure we don't open original graph
      poolClient.checkoutSynchronized(gUri.asString(), msg -> { // update the path to newUri of copied resource
        String uri = msg.body();
        String path = new GraphUri(uri).getUriPath();
        checkPath(path);

        log.info("Got copy of Neo4j graph at path={}", path);
        // Set other settings using prefix "blueprints.neo4j.conf"
        gUri.getConfig().setProperty(BLUEPRINTS_NEO4J_DIRECTORY, path);
        gUri.getConfig().setProperty(POOL_RESOURCE_URI, uri);
      });
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