/**
 * 
 */
package net.deelam.graphtools.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.google.common.base.Preconditions;
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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;
import net.deelam.graphtools.IdGraphFactory;
import net.deelam.graphtools.JsonPropertyMerger;
import net.deelam.graphtools.PropertyMerger;
import net.deelam.graphtools.util.IdUtils;

/**
 * @author deelam
 * 
 */
@Slf4j
public class IdGraphFactoryTitan implements IdGraphFactory {

  /// GraphUri configuration parameters
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
//    +TITAN_PROPS_FILE+"=titan1.props"
    );
    if (guri.exists()) {
      IdGraph<?> graph = guri.openIdGraph();
      System.out.println(GraphUtils.toString(graph));
      guri.shutdown();
      System.out.println("Shutdown");
      System.out.println(guri.exists());
    }
  }

  public void init(GraphUri gUri) {
    String tablename = gUri.getUriPath();
    checkUriPath(tablename);

    // check that Titan tablename is valid
    String safeTablename = IdUtils.convertToSafeChars(tablename, 500);
    if (!tablename.equals(safeTablename)) {
      throw new RuntimeException("Unsafe tablename for HBase: " + tablename);
    }
  }

  private void checkUriPath(String path) {
    if (path == null || path.equals("/")) {
      throw new IllegalArgumentException("Provide a graphname like so: 'titan:graphname'");
    }
  }

  //  private static final String CONFIG_PREFIX = "blueprints.titan.";

  @SuppressWarnings("unchecked")
  @Override
  public <T extends KeyIndexableGraph> IdGraph<T> open(GraphUri gUri) {
    String tablename = gUri.getUriPath();
    // open graph
    log.debug("Opening Titan graph with name={}", tablename);
    //GraphUri.printConfig(conf);
    try {
      TitanGraph tgraph = TitanFactory.open(getConfig(gUri).htConfigs.getTitanConfig());
      return (IdGraph<T>) toIdGraph(tgraph);
    } catch (ConfigurationException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  // http://s3.thinkaurelius.com/docs/titan/0.5.0/advanced-blueprints.html
  private IdGraph<TitanGraph> toIdGraph(TitanGraph g)
      throws FileNotFoundException, ConfigurationException, IOException {
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
    return new IdGraph<>(g, true, false);
  }

  ///

  private TitanHBaseGraphUriConfig getConfig(GraphUri gUri) {
    TitanHBaseGraphUriConfig configs = (TitanHBaseGraphUriConfig) gUri.getConfig().getProperty("CONFIGS");
    if (configs == null) {
      try {
        configs = new TitanHBaseGraphUriConfig(gUri);
        gUri.getConfig().setProperty("CONFIGS", configs);
      } catch (ConfigurationException | IOException e) {
        throw new RuntimeException(e);
      }
    }
    return configs;
  }

  private static class TitanHBaseGraphUriConfig {
    final HadoopTitanConfigs htConfigs;
    
    @AllArgsConstructor
    private static class PropNames {
      String titanProp; // property name to read
      
      String hadoopProp;// property name to read
    }

    private static final Map<String, PropNames> GURI_PROPNAME_MAP = new HashMap<>();

    static {
      GURI_PROPNAME_MAP.put(CLUSTER_HOST, new PropNames(
          HadoopTitanConfigs.STORAGE_HOSTNAME,
          "hbase.zookeeper.quorum"));
      GURI_PROPNAME_MAP.put(CLUSTER_PORT, new PropNames(
          "hbase.zookeeper.property.clientPort", // use the hbase-specific property to avoid WARNING 
          "hbase.zookeeper.property.clientPort"));
    }

    // for properties that should be passed to TitanFactory
    private static final String GURI_TITAN_PREFIX = "t.";

    final boolean DEBUG = true;

    public TitanHBaseGraphUriConfig(GraphUri gUri) throws ConfigurationException, FileNotFoundException, IOException {
      htConfigs=new HadoopTitanConfigs(
         gUri.getConfig().getString(TITAN_PROPS_FILE), 
         gUri.getConfig().getString(HADOOP_PROPS_FILE));
      
      /// load all configs
      String tablename = gUri.getUriPath();
      htConfigs.loadConfigs(tablename);
      htConfigs.setMissingTitanProperties();
      
      Configuration titanConf = htConfigs.getTitanConfig();
      org.apache.hadoop.conf.Configuration hadoopConf = htConfigs.getHadoopConfig();

      // pass graphUri configs to titanConfig
      for (@SuppressWarnings("unchecked")
      Iterator<String> itr = gUri.getConfig().getKeys(GURI_TITAN_PREFIX); itr.hasNext();) {
        String key = itr.next();
        titanConf.setProperty(key.substring(GURI_TITAN_PREFIX.length()), gUri.getConfig().getProperty(key));
      }

      // determine final value and propagate to other configs 
      GURI_PROPNAME_MAP.forEach((key, propNames) -> {
        String val = gUri.getConfig().getString(key);
        if (val == null) {
          val = titanConf.getString(propNames.titanProp);
          if (val == null)
            val = hadoopConf.get(propNames.hadoopProp);
          Preconditions.checkArgument(val != null, "Cannot find setting for " + key);
          //not needed: gUri.getConfig().setProperty(key, val);
        }

        /// set hadoopConf (for use by other methods) and titanConf (for opening graph)
        if (DEBUG) {
          String titanVal = titanConf.getString(propNames.titanProp);
          if (titanVal != null && !titanVal.equals(val))
            log.info("Overridding Titan config {}={} with {}", propNames.titanProp, titanVal, val);

          String hadoopVal = hadoopConf.get(propNames.hadoopProp);
          if (hadoopVal != null && !hadoopVal.equals(val))
            log.info("Overridding Hadoop config {}={} with {}", propNames.hadoopProp, hadoopVal, val);
        }

        titanConf.setProperty(propNames.titanProp, val);
        hadoopConf.set(propNames.hadoopProp, val);
        log.info("Set {} and {} to {}", propNames.titanProp, propNames.hadoopProp, val);
      });

      //log.info("Connecting to Titan with tablename={} at host='{}' using backend={}", tablename, hostname, backendType);
    }

  };


  @Override
  public void delete(GraphUri gUri) throws IOException {
    TitanGraph tgraph = (TitanGraph) gUri.getGraph().getBaseGraph();
    TitanCleanup.clear(tgraph);

    // or delete HBase table
    //    String tablename = gUri.getUriPath();
    //    getHdfsUtils().deleteHBaseTable(tablename);
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
      TitanHBaseGraphUriConfig config = getConfig(gUri);
      String backendType = config.htConfigs.getTitanConfig().getString(HadoopTitanConfigs.STORAGE_BACKEND);
      if (backendType.equals("hbase")) {
        String tablename = gUri.getUriPath();
        return config.htConfigs.getHdfsUtils().hasTable(tablename);
      } else
        return false;
    } catch (IOException e) {
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