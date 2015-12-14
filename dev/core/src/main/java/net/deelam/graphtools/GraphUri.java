/**
 * 
 */
package net.deelam.graphtools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.google.common.base.Preconditions;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

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
@RequiredArgsConstructor
@ToString
@Slf4j
public class GraphUri {
  
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
  private IdGraphFactory factory;

  public GraphUri(String uri) {
    this(uri, new BaseConfiguration());
  }

  String origUri;
  public String asString() throws URISyntaxException {
    return origUri;
  }

  public GraphUri(String uri, Configuration config) {
    Preconditions.checkNotNull(uri, "uri parameter cannot be null");
    origUri=uri;
    int colonIndx = uri.indexOf(':');
    Preconditions.checkState(colonIndx>0, "Expecting something like 'tinker:'");
    scheme=uri.substring(0,colonIndx);
    factory = graphFtry.get(scheme);
    Preconditions.checkNotNull(factory, "Unknown schema: " + scheme);
    
    baseUri=URI.create(uri.substring(colonIndx+1));
    this.config = config;
    parseUriPath(baseUri);
  }

  @Getter
  private Configuration config;

  /**
   * Open existing or create a new graph
   * @return
   */
  @SuppressWarnings("rawtypes")
  public IdGraph openIdGraph() {
    return openIdGraph(KeyIndexableGraph.class);
  }
  
  public boolean exists(){
    return factory.exists(this);
  }
  
  @SuppressWarnings("rawtypes")
  public IdGraph openExistingIdGraph() throws FileNotFoundException {
    if(factory.exists(this)){
      return openIdGraph(KeyIndexableGraph.class);
    } else {
      throw new FileNotFoundException("Graph not found at "+getUriPath());
    }
  }
  
  /**
   * Create a new empty graph, deleting any existing graph if deleteExisting=true
   * @return
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  public IdGraph createNewIdGraph(boolean deleteExisting) throws IOException {
    if(factory.exists(this)){
      if(deleteExisting)
        factory.delete(this);
      else
        throw new FileAlreadyExistsException("Graph exists at: "+getUriPath());
    }
    IdGraph<KeyIndexableGraph> graph = openIdGraph(KeyIndexableGraph.class);
    return graph;
  }
  
  public boolean delete() throws IOException{
    if(factory.exists(this)){
      factory.delete(this);
      return true;
    } else {
      return false;
    }
  }

  @Getter
  private IdGraph graph;
  public void shutdown(){
    if(graph!=null){
      shutdown(graph);
    } else {
      new IllegalArgumentException("Call shutdown(graph) instead since you didn't open the graph using this class.");
    }
  }
  
  public void shutdown(IdGraph<?> graph){
    log.info("Shutting down graph={}",graph);
    try {
      factory.shutdown(this, graph);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Open existing or create a new graph
   * @param baseGraphClass
   * @return
   */
  public <T extends KeyIndexableGraph> IdGraph<T> openIdGraph(Class<T> baseGraphClass) {
    if (config == null)
      config = new BaseConfiguration();
    config.setProperty(URI_SCHEMA_PART, baseUri.getSchemeSpecificPart());
    parseQuery(baseUri.toString());
    graph = factory.open(this);
    log.info("Opened graph={}", graph);
    {
      GraphUtils.addMetaDataNode(this, graph);
    }
    return graph;
  }

  public static void printConfig(Configuration conf) {
    for (@SuppressWarnings("unchecked")
    Iterator<String> itr = conf.getKeys(); itr.hasNext();) {
      String key = itr.next();
      System.out.println(key + "=" + conf.getString(key));
    }
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
        String[] pair = kv.split("=");
        config.setProperty(pair[0], pair[1]);
      }
    }
  }

  private static Map<String, IdGraphFactory> graphFtry = new HashMap<>(5);

  public static void register(String scheme, IdGraphFactory factory) {
    graphFtry.put(scheme, factory);
  }

  public URI getUri() {
    return baseUri;
  }
  public String getUriPath() {
    return getConfig().getString(URI_PATH);
  }

  public String[] getVertexTypes() {
    String[] types=new String[0];
    // TODO Auto-generated method stub
    return types;
  }

  public String[] getEdgeLabels() {
    String[] labels=new String[0];
    return labels;
  }

}
