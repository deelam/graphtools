/**
 * 
 */
package net.deelam.graphtools.hadoop;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;
import net.deelam.graphtools.IdGraphFactory;
import net.deelam.graphtools.JsonPropertyMerger;
import net.deelam.graphtools.PropertyMerger;
import net.deelam.graphtools.util.IdUtils;
import net.deelam.graphtools.util.PropertiesUtils;

/**
 * @author deelam
 * 
 */
@Slf4j
public class IdGraphFactoryTitan implements IdGraphFactory {

  private static final String CLUSTER_HOST = "clusterHost";
  private static final String CLUSTER_PORT = "clusterPort";
  private static final String HADOOP_PROPS_FILE = "hadoopPropsFile";
  private static final String TITAN_PROPS_FILE = "titanPropsFile";

  @Override
  public String getScheme() {
    return "titan";
  }

  public static void register() {
    GraphUri.register(new IdGraphFactoryTitan());
  }

  public static void main(String[] args) throws InterruptedException {
    final String DEFAULT_GRAPHNAME = "tmp-adidis-sessions-query1-__-src1-20803-aisd-telephone.csv"; //"dnlam-testGraph3";
    IdGraphFactoryTitan.register();

    GraphUri guri = new GraphUri("titan:" + DEFAULT_GRAPHNAME + "?"
        + CLUSTER_HOST + "=luster4&"
    + HADOOP_PROPS_FILE + "=hadoop2.props.old&"
    //+TITAN_PROPS_FILE+"=titan.props"
    );
    if (guri.exists()) {
      IdGraph<?> graph = guri.openIdGraph();
      System.out.println(GraphUtils.toString(graph));
      guri.shutdown();
      System.out.println("Shutdown");
      System.out.println(guri.exists());
    }
  }

  //  private static final String CONFIG_PREFIX = "blueprints.titan.";

  @SuppressWarnings("unchecked")
  @Override
  public <T extends KeyIndexableGraph> IdGraph<T> open(GraphUri gUri) {
    String tablename = gUri.getUriPath();
    // open graph
    checkPath(tablename);
    log.debug("Opening Titan graph with name={}", tablename);
    //GraphUri.printConfig(conf);
    try {
      return (IdGraph<T>) openTitanGraph(gUri);
    } catch (ConfigurationException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static final String STORAGE_BACKEND = "storage.backend";
  public static final String STORAGE_HOSTNAME = "storage.hostname";
  public static final String STORAGE_PORT = "storage.port";
  public static final String STORAGE_HBASE_TABLE = "storage.hbase.table";

  private IdGraph<?> openTitanGraph(GraphUri gUri) throws ConfigurationException, FileNotFoundException, IOException {
    TitanGraph tgraph = TitanFactory.open(getTitanConfig(gUri));
    IdGraph<TitanGraph> graph = new IdGraph<>(asIdGraph(tgraph), true, false);
    return graph;
  }

  private Configuration titanConfig = null;
  private Configuration getTitanConfig(GraphUri gUri)
      throws ConfigurationException, FileNotFoundException, IOException {
    if (titanConfig == null) {
      // check that Titan tablename is valid
      String tablename = gUri.getUriPath();
      String safeTablename = IdUtils.convertToSafeChars(tablename, 500);
      if (!tablename.equals(safeTablename)) {
        throw new RuntimeException("Unsafe tablename for HBase: " + tablename);
      }

      titanConfig = new BaseConfiguration();
      {
        // load titan.props
        incorporateTitanProps(gUri.getConfig().getString(TITAN_PROPS_FILE), titanConfig);
  
        // load hadoop.props and inferMissingTitanProperties
        incorporateHadoopConfig(gUri, gUri.getConfig().getString(HADOOP_PROPS_FILE), titanConfig);
  
        // then override settings based on gUri
        setTitanConfig(gUri, titanConfig);
      }
    }
    return titanConfig;
  }

  // http://s3.thinkaurelius.com/docs/titan/0.5.0/advanced-blueprints.html
  private TitanGraph asIdGraph(TitanGraph g) throws FileNotFoundException, ConfigurationException, IOException {
    // Define a property key and index for IdGraph-managed vertex IDs
    TitanManagement mgmt = g.getManagementSystem();

    TitanGraphIndex idGraphIndex = mgmt.getGraphIndex("vByIdString");
    if (idGraphIndex == null) {
      PropertyKey idGraphId =
          mgmt.makePropertyKey(IdGraph.ID).dataType(String.class).cardinality(Cardinality.SINGLE).make();
      mgmt.buildIndex("vByIdString", Vertex.class).addKey(idGraphId).unique().buildCompositeIndex(); //buildInternalIndex();
      // IllegalArgumentException: Unique indexes can only be created on vertices: mgmt.buildIndex("byeid", Edge.class).addKey(id).unique().buildCompositeIndex();
    }
    //other index options: http://s3.thinkaurelius.com/docs/titan/0.5.0/indexes.html
    // Also checkout https://github.com/thinkaurelius/titan/wiki/Advanced-Indexing
    mgmt.commit();
    return g;
  }

  // for properties that should be passed to TitanFactory
  private static final String GURI_TITAN_PREFIX = "t.";

  // create configuration with only Titan-specific entries, otherwise Exceptions are logged
  private void setTitanConfig(GraphUri gUri, Configuration titanConfig) {
    for (@SuppressWarnings("unchecked")
    Iterator<String> itr = gUri.getConfig().getKeys(GURI_TITAN_PREFIX); itr.hasNext();) {
      String key = itr.next();
      titanConfig.setProperty(key.substring(GURI_TITAN_PREFIX.length()), gUri.getConfig().getProperty(key));
    }

    String port = gUri.getConfig().getString(CLUSTER_PORT);
    if (port != null) {
      String existing = titanConfig.getString(STORAGE_PORT);
      if(existing!=null)
        log.info("Overriding {}={} with {}", STORAGE_PORT, existing, port);
      titanConfig.setProperty(STORAGE_PORT, port);
      log.debug("Setting {}={}", STORAGE_PORT, port);
    }
    
    String host = gUri.getConfig().getString(CLUSTER_HOST);
    if (host != null) {
      String existing = titanConfig.getString(STORAGE_HOSTNAME);
      if(existing!=null)
        log.info("Overriding {}={} with {}", STORAGE_HOSTNAME, existing, host);
      titanConfig.setProperty(STORAGE_HOSTNAME, host);
      log.debug("Setting {}={}", STORAGE_HOSTNAME, host);
    }
    String hostname = titanConfig.getString(STORAGE_HOSTNAME);
    checkNotNull(hostname, "Must set cluster host via GraphUri, prop file, or hadoop 'yarn-conf' directory");

    String tablename = gUri.getUriPath();
    titanConfig.setProperty(STORAGE_HBASE_TABLE, tablename);
    log.debug("Setting {}={}", STORAGE_HBASE_TABLE, tablename);

    /// Set sensible defaults
    if (!titanConfig.containsKey(STORAGE_BACKEND)) { // then assume HBase
      // http://s3.thinkaurelius.com/docs/titan/0.5.1/configuration.html
      titanConfig.setProperty(STORAGE_BACKEND, "hbase");
    }
    String backendType = titanConfig.getString(STORAGE_BACKEND);
    if (backendType.equals("hbase") && !titanConfig.containsKey("storage.hbase.compat-class")) {
      // Use 0.98 until HBase 1.0.0 support is added to Titan
      titanConfig.setProperty("storage.hbase.compat-class",
          "com.thinkaurelius.titan.diskstorage.hbase.HBaseCompat0_98");
      // without this, you'll get an "Unrecognized or unsupported HBase version 1.0.0-cdh5.4.4" exception
    }

    log.info("Connecting to Titan with tablename={} at host='{}' using backend={}", tablename, hostname, backendType);
  }

  private static void incorporateTitanProps(String titanPropsFile, Configuration titanConfig)
      throws ConfigurationException, FileNotFoundException, IOException {
    if (titanPropsFile != null) {
      Properties titanProps = new Properties();
      PropertiesUtils.loadProperties(titanPropsFile, titanProps);
      for (Entry<Object, Object> e : titanProps.entrySet()) {
        Object existingV = titanConfig.getProperty((String) e.getKey());
        if (existingV != null && !e.getValue().equals(existingV))
          log.warn("Skipping property file entry: {}={}.  Using GraphUri's existing config value:" + existingV,
              e.getKey(), e.getValue());
        else {
          log.debug("Titan config property {}={}", e.getKey(), e.getValue());
          titanConfig.setProperty((String) e.getKey(), e.getValue());
        }
      }
    }
  }

  private void incorporateHadoopConfig(GraphUri gUri, String hadoopPropsFile, Configuration titanConfig)
      throws ConfigurationException, FileNotFoundException, IOException {
    if (hadoopPropsFile != null) {
      //      Properties hadoopProps = PropertiesUtils.loadProperties(hadoopPropsFile, new Properties());
      //      org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
//FIXME
      inferMissingTitanProperties(titanConfig, getHadoopConfig(gUri));
    }
  }

  private org.apache.hadoop.conf.Configuration hadoopConf;
  private org.apache.hadoop.conf.Configuration getHadoopConfig(GraphUri gUri) throws ConfigurationException {
    if(hadoopConf==null){
      String hadoopPropsFile=gUri.getConfig().getString(HADOOP_PROPS_FILE);
      Properties conf = HadoopConfigurationHelper.getHadoopBaseConfiguration(hadoopPropsFile);
      //    conf.setProperty("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
      //    conf.setProperty("fs.file.impl", LocalFileSystem.class.getName());
      hadoopConf = HadoopConfigurationHelper.toHadoopConfig(conf);
    }
    return hadoopConf;
  }
  
  private static void inferMissingTitanProperties(Configuration titanConf,
      org.apache.hadoop.conf.Configuration hadoopConf) {
    final String hostname = titanConf.getString(STORAGE_HOSTNAME);
    String hbaseZks = hadoopConf.get("hbase.zookeeper.quorum");
    if (hostname == null) {
      if (hbaseZks == null) {
        throw new RuntimeException("Need to specify Titan-HBase location by setting hbase.zookeeper.quorum or "
            + STORAGE_HOSTNAME);
      } else {
        titanConf.setProperty(STORAGE_HOSTNAME, hbaseZks);
        log.info("Inferring {}={}", STORAGE_HOSTNAME, hbaseZks);
      }
    } else if (!hostname.equals(hbaseZks)){
      log.warn("For {}, using specified '{}' instead of '{}' from property file",STORAGE_HOSTNAME, hostname, hbaseZks);
    }
  }

  protected void checkPath(String path) {
    if (path == null || path.equals("/")) {
      throw new IllegalArgumentException("Provide a graphname like so: 'titan:graphname'");
    }
  }

  @Override
  public void delete(GraphUri gUri) throws IOException {

    TitanGraph tgraph = (TitanGraph) gUri.getGraph().getBaseGraph();
    TitanCleanup.clear(tgraph);

    // or delete HBase table
    //    String tablename = gUri.getUriPath();
    //    getHdfsUtils().deleteHBaseTable(tablename);
  }

  private HdfsUtils hdfsUtils;

  private HdfsUtils getHdfsUtils(GraphUri gUri) throws ConfigurationException {
    if (hdfsUtils == null) {
      hdfsUtils = new HdfsUtils(getHadoopConfig(gUri));
    }
    return hdfsUtils;
  }

  @Override
  public boolean exists(GraphUri gUri) {
    /*
    TitanGraph tgraph = (TitanGraph) gUri.getGraph().getBaseGraph();
    StandardTitanGraph stgraph = (StandardTitanGraph) tgraph;
    final GraphDatabaseConfiguration config = stgraph.getConfiguration();
    String descrip = config.getBackendDescription();
    log.info("backendDescription={}", descrip);
    */
    try {
      String backendType = getTitanConfig(gUri).getString(STORAGE_BACKEND);
      if (backendType.equals("hbase")) {
        String tablename = gUri.getUriPath();
        return getHdfsUtils(gUri).hasTable(tablename);
      } else
        return false;
    } catch (ConfigurationException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void backup(GraphUri srcGraphUri, GraphUri dstGraphUri) throws IOException {
    throw new UnsupportedOperationException("Cannot backup: " + srcGraphUri);
  }

  @Override
  public PropertyMerger createPropertyMerger() {
    return new JsonPropertyMerger();
  }

}
