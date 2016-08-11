/**
 * 
 */
package net.deelam.graphtools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphIndexConstants.PropertyKeys;

/**
 * GraphUri examples:
 * <li> "tinker:": in-memory TinkerGraph 
 * <li> "tinker:/tmp/testGraphs/tGraph": on-disk TinkerGraph in absolute directory
 * <li> "tinker:./testGraphs/tGraph": on-disk TinkerGraph in relative directory
 * <li> "tinker:./testGraphs/tGraphML?fileType=graphml": on-disk TinkerGraph in GraphML format
 * 
 * See associated unit tests.
 * 
 * @author deelam
 */
@ToString
@Slf4j
public class GraphUri {
  
  public static final String CREATE_META_DATA_NODE = "createMetaDataNode";

  public static void main(String[] args) {
    GraphUri graphU = new GraphUri("titan:tablename");
    System.out.println(graphU.getScheme()+" "+graphU.getUri()+" "+graphU.getUriPath());
    
    graphU = new GraphUri("graphml:./tablename.graphml");
    System.out.println(graphU.getScheme()+" "+graphU.getUri()+" "+graphU.getUriPath());
  }
  
  private static final String URI_PATH = "_uriPath";
  public static final String URI_SCHEMA_PART = "_uriSchemaSpecificPart";

  @Getter
  private final String scheme;
  private final URI baseUri;
  
  public IdGraphFactory getFactory() {
    IdGraphFactory factory = graphFtry.get(scheme);
    Preconditions.checkNotNull(factory, "Unknown schema: " + scheme);
    return factory;
  }

  public GraphUri(String uri) {
    this(uri, new BaseConfiguration());
  }

  String origUri;
  public String asString() {
    String str = getFactory().asString(this);
    return str;
  }
  
  @Override
  public boolean equals(Object arg) {
    if (arg instanceof GraphUri)
      return origUri.equals(((GraphUri) arg).origUri);
    return false;
  }
  
  @Override
  public int hashCode() {
    return origUri.hashCode();
  }

  public GraphUri(String uri, Configuration config) {
    URI.create(uri);
    Preconditions.checkNotNull(uri, "uri parameter cannot be null");
    origUri=uri;
    int colonIndx = uri.indexOf(':');
    Preconditions.checkState(colonIndx>0, "Expecting something like 'tinker:' but got "+uri);
    scheme=uri.substring(0,colonIndx);
    baseUri=URI.create(uri.substring(colonIndx+1));
    this.config = config;
    parseUriPath(baseUri);
    parseQuery(baseUri.toString());
    
    getFactory().init(this);
  }

  @Getter
  private final Configuration config;

  /**
   * Open existing or create a new graph
   * @return
   */
  @SuppressWarnings("rawtypes")
  public IdGraph openIdGraph() {
    return openIdGraph(KeyIndexableGraph.class);
  }
  
  public boolean exists(){
    return getFactory().exists(this);
  }
  
  public boolean isOpen(){
    return graph!=null;
  }
  
  @SuppressWarnings("rawtypes")
  public IdGraph openExistingIdGraph() throws FileNotFoundException {
    checkNotOpen();

    if(getFactory().exists(this)){
      return openIdGraph(KeyIndexableGraph.class);
    } else {
      throw new FileNotFoundException("Graph not found at "+getUriPath());
    }
  }

  private void checkNotOpen() {
    if(isOpen())
      throw new RuntimeException("Graph is open: {}"+graph);
  }
  
  /**
   * Create a new empty graph, deleting any existing graph if deleteExisting=true
   * @return
   * @throws IOException
   */
  public <T extends KeyIndexableGraph> IdGraph<T> createNewIdGraph(boolean deleteExisting) throws IOException {
    checkNotOpen();
    if(getFactory().exists(this)){
      if(deleteExisting)
        getFactory().delete(this);
      else
        throw new FileAlreadyExistsException("Graph exists at: "+getUriPath());
    }
    graph = openIdGraph(KeyIndexableGraph.class);
    return graph;
  }
  
  public boolean delete() throws IOException{
    checkNotOpen();
    if(getFactory().exists(this)){
      getFactory().delete(this);
      return true;
    } else {
      return false;
    }
  }

  @SuppressWarnings("rawtypes")
  @Getter
  private IdGraph graph;
  
  public IdGraph<?> getOrOpenGraph() throws FileNotFoundException{
    if(graph!=null)
      return graph;
    return openExistingIdGraph();
  }
  
  public void shutdown(){
    if(graph!=null){
      shutdown(graph);
      graph=null;
    } else {
      //new IllegalArgumentException("Call shutdown(graph) instead since you didn't open the graph using this class.");
      log.warn("Cannot shutdown; graph is not opened or you didn't open the graph using this GraphUri instance.", new Throwable("Call stack"));
    }
  }
  
  private void shutdown(IdGraph<?> graph){
    log.info("Shutting down graph={}",graph);
    try {
      getFactory().shutdown(this, graph);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      log.debug("  Shut down graphUri={}", this);
      System.gc(); // addresses problem with NFS files still being held by JVM 
    }
  }

  @Setter
  private Consumer<IdGraph<?>> openHook=null;
  
  /**
   * Open existing or create a new graph
   * @param baseGraphClass
   * @return
   */
  @SuppressWarnings("unchecked")
  public <T extends KeyIndexableGraph> IdGraph<T> openIdGraph(Class<T> baseGraphClass) {
    checkNotOpen();
    config.setProperty(URI_SCHEMA_PART, baseUri.getSchemeSpecificPart());
    log.info("Opening graphUri={} using {}", this, getFactory());
    //printConfig(config);
    try{
      graph = getFactory().open(this);
      
      if(!isReadOnly()){
        boolean createMetaDataNode = config.getBoolean(CREATE_META_DATA_NODE, true);
        log.debug("  Opened graph={}, createMetaDataNode={}", graph, createMetaDataNode);
        if(createMetaDataNode)
          GraphUtils.addMetaDataNode(this, graph);
      }

      if(openHook!=null)
        openHook.accept(graph);
    }catch(RuntimeException re){
      log.error("Could not open graphUri="+this, re);
      throw re;
    }
    return graph;
  }

  @SuppressWarnings("unchecked")
  public static void printConfig(Configuration config) {
    StringBuilder sb=new StringBuilder();
    for (Iterator<String> itr = config.getKeys(); itr.hasNext();) {
      String key = itr.next();
      sb.append("\n").append(key).append("=").append(config.getString(key));
    }
    log.info("config=", sb);
  }

  private void parseUriPath(URI uri) {
    String path = uri.getPath();
    if(path==null){
      if(uri.getScheme()==null)
        return;
      else{ // try to get path after removing scheme
        String ssp = uri.getSchemeSpecificPart();
        path=URI.create(ssp).getPath();
        if(path==null)
          return;
      }
    }
    Preconditions.checkNotNull(path);
    // check if path is relative
    if (path.length() > 1 && path.startsWith("/."))
      path = path.substring(1); // remove first char, which is '/'
    config.setProperty(URI_PATH, path);
  }

  private void parseQuery(String uriStr) {
    int queryIndx = uriStr.indexOf('?');
    if(queryIndx<0)
      return;
    String queryStr=uriStr.substring(queryIndx+1);
    if (queryStr != null) {
      for (String kv : queryStr.split("&")) {
        if(kv.length()>0){
          String[] pair = kv.split("=");
          if(config.containsKey(pair[0]))
            log.warn("Overriding configuration {}={} with {}", pair[0], config.getProperty(pair[0]), pair[1]);
          config.setProperty(pair[0], pair[1]);
        }
      }
    }
  }

  private static Map<String, IdGraphFactory> graphFtry = new HashMap<>(5);

  public static void register(String scheme, IdGraphFactory factory) {
    if(graphFtry.containsKey(scheme))
      log.warn("Replacing existing IdGraphFactory for {} with {}", scheme, factory);
    graphFtry.put(scheme, factory);
    log.info("Registering {} with {}", scheme, factory);
  }

  public static void register(IdGraphFactory factory) {
    register(factory.getScheme(), factory);
  }

  public URI getUri() {
    return baseUri;
  }
  public String getUriPath() {
    return config.getString(URI_PATH);
  }

  public void backupTo(GraphUri dstGraphUri) throws IOException {
    if(this.isOpen())
      throw new IllegalStateException("Source graph must not be open so underlying files can be copied.");
    if(dstGraphUri.isOpen())
      throw new IllegalStateException("Destination graph must not be open so underlying files can be copied.");
    if(dstGraphUri.exists())
      throw new IllegalStateException("Destination graph must not already exist so underlying files can be copied.");
    getFactory().backup(this, dstGraphUri);
  }

  public PropertyMerger createPropertyMerger() {
    return getFactory().createPropertyMerger();
  }

  public GraphUri setConfig(String key, Object value) {
    config.setProperty(key, value);
    return this;
  }

  public GraphUri readOnly() {
    return setConfig(IdGraphFactory.READONLY, true);
  }
  
  public boolean isReadOnly() {
    return config.getBoolean(IdGraphFactory.READONLY, false);
  }

  public void createIndices(PropertyKeys pks) {
    if(!this.isOpen())
      throw new IllegalStateException("Graph must not be open to create indices.");
    getFactory().createIndices(this, graph, pks);
  }

// use GraphUtils.addMetaData() instead
//  public String[] getVertexTypes() {
//    String[] types=new String[0];
//    // getVertexTypes
//    return types;
//  }
//
//  public String[] getEdgeLabels() {
//    String[] labels=new String[0];
//    return labels;
//  }

}
