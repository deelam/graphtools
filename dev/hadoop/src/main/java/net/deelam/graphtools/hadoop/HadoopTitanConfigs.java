package net.deelam.graphtools.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.util.PropertiesUtils;

@RequiredArgsConstructor
@Slf4j
public class HadoopTitanConfigs {
  final String titanPropsFile;
  final String hadoopPropsFile;

  public void loadConfigs(String titanTablename) throws ConfigurationException, FileNotFoundException, IOException {
    /// load all configs
    loadHadoopConfig();
    loadTitanConfig(titanTablename);
  }

  public static final String STORAGE_HOSTNAME = "storage.hostname";
  public static final String STORAGE_PORT = "storage.port";
  public static final String STORAGE_HBASE_TABLE = "storage.hbase.table";
  public static final String STORAGE_BACKEND = "storage.backend";

  private Configuration titanConfig = null;
  public Configuration getTitanConfig() {
    if(titanConfig==null){
      throw new IllegalStateException("Must call getTitanConfig(tablename) or loadConfigs(titanTablename) first");
    }
    return titanConfig;
  }
  public Configuration getTitanConfig(String tablename) throws ConfigurationException, FileNotFoundException, IOException {
    return loadTitanConfig(tablename);
  }

  // create configuration with only Titan-specific entries, otherwise Exceptions are logged
  private Configuration loadTitanConfig(String tablename)
      throws ConfigurationException, FileNotFoundException, IOException {
    if (titanConfig == null) {
      titanConfig = new BaseConfiguration();
      if (titanPropsFile != null) {
        Properties titanProps = new Properties();
        PropertiesUtils.loadProperties(titanPropsFile, titanProps);
        for (Entry<Object, Object> e : titanProps.entrySet()) {
          titanConfig.setProperty((String) e.getKey(), e.getValue());
        }
      }

      /// Set sensible defaults
      if (!titanConfig.containsKey(STORAGE_BACKEND)) { // then assume HBase
        // http://s3.thinkaurelius.com/docs/titan/0.5.1/configuration.html
        titanConfig.setProperty(STORAGE_BACKEND, "hbase");
      }
      String backendType = titanConfig.getString(STORAGE_BACKEND);
      if (backendType.equals("hbase")) {
        if (!titanConfig.containsKey("storage.hbase.compat-class")) {
          // Use 0.98 until HBase 1.0.0 support is added to Titan
          titanConfig.setProperty("storage.hbase.compat-class",
              "com.thinkaurelius.titan.diskstorage.hbase.HBaseCompat0_98");
          // without this, you'll get an "Unrecognized or unsupported HBase version 1.0.0-cdh5.4.4" exception
        }
        String port = titanConfig.getString(STORAGE_PORT);
        if (port != null) {
          log.info("Copying {}={} to hbase.zookeeper.property.clientPort property to avoid Titan warning.");
          titanConfig.setProperty("hbase.zookeeper.property.clientPort", port);
        }
      }
    }
    if(tablename!=null){
      String existing = titanConfig.getString(STORAGE_HBASE_TABLE);
      if(existing!=null && !existing.equals(tablename))
        log.warn("Overriding existing tablename={} with {}", existing, tablename);
      titanConfig.setProperty(STORAGE_HBASE_TABLE, tablename);
      log.debug("Setting {}={}", STORAGE_HBASE_TABLE, tablename);
    }

    return titanConfig;
  }

  private org.apache.hadoop.conf.Configuration hadoopConfig = null;
  public org.apache.hadoop.conf.Configuration getHadoopConfig() throws ConfigurationException{
    return loadHadoopConfig();
  }

  private org.apache.hadoop.conf.Configuration loadHadoopConfig() throws ConfigurationException {
    if (hadoopConfig == null) {
      hadoopConfig = new HadoopConfigurationHelper().loadHadoopConfig(hadoopPropsFile);
    }
    return hadoopConfig;
  }

  HdfsUtils hdfsUtils;

  public HdfsUtils getHdfsUtils() {
    if (hdfsUtils == null) {
      hdfsUtils = new HdfsUtils(hadoopConfig);
    }
    return hdfsUtils;
  }

  ///

  private static final Map<String, String> TITAN_PROPNAME_MAP = new HashMap<>();

  static {
    TITAN_PROPNAME_MAP.put(STORAGE_HOSTNAME, "hbase.zookeeper.quorum");
// Don't need to set titan property if it is set in hbase-site.xml (based on HBaseStoreManager code)
//    TITAN_PROPNAME_MAP.put(
//        STORAGE_PORT, //"storage.hbase.ext.hbase.zookeeper.property.clientPort", // use the hbase-specific property to avoid HBaseStoreManager WARNING 
//        "hbase.zookeeper.property.clientPort");
  }

  public void setMissingTitanProperties() {
    TITAN_PROPNAME_MAP.forEach((titanProp, hProp) -> {
      if(!titanConfig.containsKey(titanProp)){
        String hValue=hadoopConfig.get(hProp);
        if(hValue!=null)
          titanConfig.setProperty(titanProp, hValue);
      }
    });
  }

}
