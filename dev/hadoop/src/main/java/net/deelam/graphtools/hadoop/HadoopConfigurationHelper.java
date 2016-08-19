package net.deelam.graphtools.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.function.Supplier;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.util.ClassLoaderContext;
import net.deelam.graphtools.util.PropertiesUtils;

@Slf4j
public final class HadoopConfigurationHelper {

  Configuration loadHadoopConfig(String hadoopPropsFile) throws ConfigurationException {
    Properties hadoopProps = loadHadoopProperties(hadoopPropsFile);

    String yarnMgr = hadoopProps.getProperty("yarn.resourcemanager.hostname");
    if ("localhost".equals(yarnMgr)) {
      checkYarnApplicationClasspath(hadoopProps);
    }

    //    conf.setProperty("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    //    conf.setProperty("fs.file.impl", LocalFileSystem.class.getName());
    Configuration hadoopConfig=loadHadoopConfigDirs(hadoopProps);
    checkForMinimumSettings(hadoopConfig);
    return hadoopConfig;
  }

  private final static String[] minSettings = {
      "fs.defaultFS", 
      "hbase.zookeeper.quorum", 
      "hbase.zookeeper.property.clientPort",
      "mapreduce.framework.name", // =yarn
      "yarn.resourcemanager.hostname",
      "yarn.application.classpath" // without this, MapReduce jobs fail
      };
  
  public static void main(String[] args) {
    Configuration minConfig=new Configuration(false);
    minConfig.forEach(e->{
      System.out.println(e);
    });
  }
  
  Configuration loadMinimalHadoopConfig(String hadoopPropsFile) throws ConfigurationException{
    Properties hadoopProps = loadHadoopProperties(hadoopPropsFile);
    Configuration hadoopConfig=loadHadoopConfigDirs(hadoopProps);
    if(debug)
      HadoopConfigurationHelper.print(hadoopConfig, "dump-hadoopConfig-withProps.xml");

    /// Set minimal settings from Hadoop config dir
    // try(ClassLoaderContext cl = new ClassLoaderContext(FileSystem.class)) { // Calling 'new Configuration' sets the current classloader as its field
    Configuration minConfig = new Configuration(false); // false=don't load defaults
    minConfig.setClassLoader(FileSystem.class.getClassLoader()); // so that FileSystem uses the right classloader

    for (String s : minSettings) {
      if (hadoopConfig.get(s) != null)
        minConfig.set(s, hadoopConfig.get(s));
    };

    hadoopConfig.getValByRegex("fs..*.impl").forEach((key, value) -> {
      log.debug("Setting Hadoop's minConfig: {}={}", key, value);
      minConfig.set(key, value);
    });
    
    // Override with props
    hadoopProps.forEach((key,value)->{
      String confVal = minConfig.get((String) key);
      if(confVal!=null && !confVal.equals(value)){
        log.info("Overriding Hadoop's config {}={} with {}", key, confVal, value);
      }
      minConfig.set((String) key, (String)value);
    });

    checkForMinimumSettings(minConfig);

    if(debug)
      HadoopConfigurationHelper.print(minConfig, "dump-minConf-withProps.xml");
    
    if(debug)
      try{
        log.error("ccl="+Thread.currentThread().getContextClassLoader());
        FileSystem.getFileSystemClass("hdfs", hadoopConfig); // check that classes can be loaded
        FileSystem.getFileSystemClass("hdfs", minConfig); // check that classes can be loaded
      } catch (IOException e) {
        e.printStackTrace();
      }

    return minConfig; 
  }

  private void checkForMinimumSettings(Configuration minConfig) {
    // Check settings
    for(String s:minSettings){
      if (minConfig.get(s) == null) {
        log.warn("No setting for {}", s);
      }
    }
  }

  private Properties loadHadoopProperties(String hadoopPropsFile) throws ConfigurationException {
    Properties hadoopProps = new Properties();
    if (hadoopPropsFile == null) {
      log.info("Hadoop property file not specified; not loading file.");
    } else {
      try {
        PropertiesUtils.loadProperties(hadoopPropsFile, hadoopProps);
      } catch (IOException e1) {
        log.error("Error when reading " + hadoopPropsFile, e1);
      }
    }
    setHadoopUser(hadoopProps);
    findAndSetYarnJobJar(hadoopProps);
    return hadoopProps;
  }

  private static void checkYarnApplicationClasspath(Properties hadoopBaseConfig) {
    String cpStr = hadoopBaseConfig.getProperty("yarn.application.classpath");
    if (cpStr == null)
      return;
    log.info("Checking files in yarn.application.classpath exist ...");
    String[] cpArr = cpStr.split(":");
    for (String jarFile : cpArr) {
      int starIndex = jarFile.indexOf("*");
      if (starIndex > 0) {
        jarFile = jarFile.substring(0, starIndex);
      }
      File file = new File(jarFile);
      if (!file.exists()) {
        log.warn("File doesn't exists: " + file.getAbsolutePath());
        //            System.err.println("File doesn't exists: "+file.getAbsolutePath());
      }
    }
  }

  private static void setHadoopUser(Properties conf) {
    String hadoopUserName = conf.getProperty("env.HADOOP_USER_NAME");
    if (hadoopUserName != null) {
      /// config settings to access hadoop remotely (e.g., my desktop)
      // use hadoopUserName for connecting to the remote Hadoop cluster
      log.info("Setting environment variable HADOOP_USER_NAME={}", hadoopUserName);
      System.setProperty("HADOOP_USER_NAME", hadoopUserName);
      //checkState(hadoopUserName.equals(System.getenv().get("HADOOP_USER_NAME")));
      //log.info("env: "+System.getenv());
    } else {
      log.warn("HADOOP_USER_NAME not set; Hadoop will use current user.name={}", System.getProperty("user.name"));
    }
  }

  @Setter
  private Supplier<File> jobJarFinder=null;
  
  private void findAndSetYarnJobJar(Properties conf) {
    String jobJar = conf.getProperty("mapreduce.job.jar");
    if(jobJarFinder!=null){
      File jobJarF=jobJarFinder.get();
      log.info("Setting mapreduce.job.jar={}", jobJarF.getAbsolutePath());
      conf.setProperty("mapreduce.job.jar", jobJarF.getAbsolutePath());
    }
    if (jobJar == null) {
      log.info("mapreduce.job.jar is not set");
    }
  }

  public static final boolean debug = !true;

  /**
   * Notes regarding Configuration and OSGi:
   * Configuration has a static code block that adds xml files to load on-demand *from the classpath*
   * for all instantiations of Configuration.  *-default.xml files are loaded first.
   * In OSGi, this is complicated since the classpath is only within the bundle and
   * the bundle cannot access the 'System' classpath (specified with the -cp java option on the commandline).
   * 
   * To address this, we manually add the xml files as URL or Path resources to the Configuration instance
   * so that we don't have to rely on the classpath being correct.
   * The advantage is that this is self-contained compared to the following alternative.
   * 
   * An alternative is to create a new Configuration() while in a bundle activator class where
   * the 'Thread.currentThread().getContextClassLoader()' is the 'System' classloader which has
   * access the classpath specified with the -cp java option.
   * Disadvantage of this is dependence on an activator class.
   * 
   */
  private static Configuration loadHadoopConfigDirs(Properties props) {
    Configuration hadoopConf = new Configuration();
    {
      String yarnConfDir = props.getProperty("YARN_CONF_DIR");
      if (yarnConfDir!=null){
        if(!new File(yarnConfDir).exists()) {
          log.warn("Specified YARN_CONF_DIR={} does not exist!", yarnConfDir);
        }
      } else {
        yarnConfDir="./yarn-conf";
      }
      if (new File(yarnConfDir).exists()) {
        log.info("Using YARN_CONF_DIR={}", yarnConfDir);
        importFilesToConfig(yarnConfDir, hadoopConf, "xml");

        if (!props.containsKey("mapreduce.framework.name")) {
          log.info("Since YARN_CONF_DIR exists, inferring missing property: mapreduce.framework.name=yarn");
          props.setProperty("mapreduce.framework.name", "yarn");
        }
      }
    }
    
    // infer "yarn.resourcemanager.hostname"
    String yarnRmHostname = hadoopConf.get("yarn.resourcemanager.hostname");
    if(yarnRmHostname==null || "0.0.0.0".equals(yarnRmHostname)){
      String rmAddr = hadoopConf.get("yarn.resourcemanager.address"); // e.g., "luster5.arlut.utexas.edu:8032"
      if(rmAddr!=null){
        log.debug("YARN addr={}", rmAddr);
        int colon=rmAddr.indexOf(':');
        if(colon>0){
          String hostname=rmAddr.substring(0, colon);
          if(!"0.0.0.0".equals(hostname)){
            log.info("Inferring yarn.resourcemanager.hostname={} from {}", hostname, rmAddr);
            hadoopConf.set("yarn.resourcemanager.hostname", hostname);
          }
        }
      }
    }
    
    // HBaseConfiguration.create() loads hbase-default.xml found by HBaseConfiguration's classloader and 
    // calls "new Configuration()", 
    // which loads core-default.xml and hadoop-site.xml, as well as resources added by addDefaultResource()

    Configuration hbaseConf = HBaseConfiguration.create(hadoopConf);
    {
      String hbaseConfDir = props.getProperty("HBASE_CONF_DIR");
      if (hbaseConfDir!=null){
        if(!new File(hbaseConfDir).exists()) {
          log.warn("Specified HBASE_CONF_DIR={} does not exist!", hbaseConfDir);
        }
      } else {
        hbaseConfDir="./hbase-conf";
      }
      if (new File(hbaseConfDir).exists()) {
        log.info("Using HBASE_CONF_DIR={}", hbaseConfDir);
        importFilesToConfig(hbaseConfDir, hbaseConf, "xml");
      }
    }

    if (debug)
      HadoopConfigurationHelper.print(hbaseConf, "dump-hbase.xml");

    log.info("  Adding properties from file to (possibly override) loaded default configuration for Hadoop and HBase");
    props.forEach((key,value)->{
      log.debug("    key: {}={}", key, value);
      hbaseConf.set((String) key, (String)value);
    });

    if (debug)
      HadoopConfigurationHelper.print(hbaseConf, "dump-hbaseConf-withProps.xml");

    //log.error("Who?", new Throwable("Who calls this?"));
    // need to be in the titan.jar so that ServiceLoader.load() can load all FileSystem impls provided in the jar
    try (ClassLoaderContext cl = new ClassLoaderContext(FileSystem.class)) {
      ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
      for (FileSystem fs : serviceLoader) {
        final String fsImplKey = "fs." + fs.getScheme() + ".impl";
        String existing = hbaseConf.get(fsImplKey);
        String propFsClass = props.getProperty(fsImplKey);
        if (propFsClass == null) {
          if(existing==null)
            log.info("  Setting dynamically discovered Hadoop config {}={}", fsImplKey, fs.getClass());
          else if(!existing.equals(fs.getClass().getName()))
            log.info("  Overriding Hadoop config {}={} with discovered {}", fsImplKey, existing, fs.getClass());
          hbaseConf.set(fsImplKey, fs.getClass().getName());
        } else {
          if(!propFsClass.equals(fs.getClass().getName()))
            log.info("  Setting {}={} from property file instead of discovered {}", fsImplKey, propFsClass, fs.getClass());
          hbaseConf.set(fsImplKey, propFsClass);
        }
      }
      //org.apache.hadoop.fs.FileSystem.getFileSystemClass("file", hbaseConf); // loadFileSystems() scans classpath for FileSystem implementations
      //log.warn("defaultFS="+org.apache.hadoop.fs.FileSystem.get(hbaseConf));
    }
    return hbaseConf;
  }

  private static void importFilesToConfig(String yarnConfDir, Configuration conf, String... fileExtensions) {
    Collection<File> xmlFiles = FileUtils.listFiles(new File(yarnConfDir), fileExtensions, false);
    for (File file : xmlFiles) {
      try {
        conf.addResource(file.toURI().toURL());
      } catch (MalformedURLException e) {
        e.printStackTrace();
      }
    }
    //log.debug("###########  {}", conf.get("fs.defaultFS"));
  }

  public static void print(Configuration conf, String filename) {
    try {
      if (filename == null)
        conf.writeXml(System.out);
      else {
        try (OutputStream out = new FileOutputStream(filename)) {
          conf.writeXml(out);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

}
