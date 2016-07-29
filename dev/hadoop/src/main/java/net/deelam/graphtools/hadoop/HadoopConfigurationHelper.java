package net.deelam.graphtools.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Properties;
import java.util.ServiceLoader;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.util.ClassLoaderContext;
import net.deelam.graphtools.util.PropertiesUtils;

@Slf4j
public class HadoopConfigurationHelper {

  static synchronized Properties getHadoopBaseConfiguration(String hadoopPropsFile)
      throws ConfigurationException {

    Properties hadoopBaseConfig=new Properties();
    if (hadoopPropsFile != null) {
      try {
         PropertiesUtils.loadProperties(hadoopPropsFile, hadoopBaseConfig);
      } catch (IOException e1) {
        log.error("Error when reading "+hadoopPropsFile, e1);
      }
    } else {
      log.info("Hadoop property file not specified; not loading file."); // for Spark jobs
    }

//    for (Entry<Object, Object> e : hadoopBaseConfig.entrySet()) {
//      Object existingV = hadoopBaseConfig.getProperty((String) e.getKey());
//      if (existingV != null && !e.getValue().equals(existingV))
//        log.warn("Overriding Hadoop config property {}={}  oldValue:" + existingV, e.getKey(), e.getValue());
//      hadoopBaseConfig.setProperty((String) e.getKey(), (String) e.getValue());
//      log.debug("Hadoop config property {}={}", e.getKey(), e.getValue());
//    }

    setHadoopUser(hadoopBaseConfig);

    setYarnJobJar(hadoopBaseConfig);

    String yarnMgr = hadoopBaseConfig.getProperty("yarn.resourcemanager.hostname");
    if ("localhost".equals(yarnMgr)) {
      checkYarnApplicationClasspath(hadoopBaseConfig);
    }

    return hadoopBaseConfig;
  }

  private static void checkYarnApplicationClasspath(Properties hadoopBaseConfig) {
    String cpStr = hadoopBaseConfig.getProperty("yarn.application.classpath");
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

  private static void setYarnJobJar(Properties conf) {
    String jobJar = conf.getProperty("mapreduce.job.jar");
    if (jobJar == null) {
      log.info("mapreduce.job.jar is not set"); 
    }
  }

  public static final boolean debug=false; // = true;

  /**
   * Notes regarding Configuration and OSGi:
   * Configuration has a static code block that adds xml files to load on-demand *from the classpath*
   * for all instantiations of Configuration.  *-default.xml files are loaded first.
   * In OSGi, this is complicated since the classpath is only within the bundle and
   * the bundle cannot access the 'System' classpath (specified with the -cp java option on the commandline).
   * 
   * To address this, we add the xml files as URL or Path resources to the Configuration instance.
   * The advantage is that this is self-contained compared to alternative below.
   * 
   * An alternative is to create a new Configuration() while in a bundle activator class where
   * the 'Thread.currentThread().getContextClassLoader()' is the 'System' classloader which has
   * access the classpath specified with the -cp java option.
   * Disadvantage of this is dependence on an activator class.
   * 
   */
  static Configuration toHadoopConfig(Properties props) {
    Configuration hadoopConf = new Configuration();
    {
      String yarnConfDir = props.getProperty("YARN_CONF_DIR", "./yarn-conf");
      if (new File(yarnConfDir).exists()) {
        log.info("Using YARN_CONF_DIR={}", yarnConfDir);
        importFilesToConfig(yarnConfDir, hadoopConf, "xml");

        log.info("Since YARN_CONF_DIR exists, setting mapreduce.framework.name=yarn");
        props.setProperty("mapreduce.framework.name", "yarn");
      }
    }

    // HBaseConfiguration.create() loads hbase-default.xml in classpath and calls "new Configuration()" 
    // which loads core-default.xml and hadoop-site.xml, as well as resources added by addDefaultResource()

    Configuration hbaseConf = HBaseConfiguration.create(hadoopConf);
    {
      String hbaseConfDir = props.getProperty("HBASE_CONF_DIR", "./hbase-conf");
      if (new File(hbaseConfDir).exists()) {
        log.info("Using HBASE_CONF_DIR={}", hbaseConfDir);
        importFilesToConfig(hbaseConfDir, hbaseConf, "xml");
      }
    }

    if (debug)
      HadoopConfigurationHelper.print(hbaseConf, "dump-hbase.xml");

    log.info("  Adding properties from file to (possibly override) loaded default configuration for Hadoop and HBase");
    for (Object keyObj : props.keySet()) {
      String key = (String) keyObj;
      String value = props.getProperty(key);
      log.debug("    key: {}={}", key, value);
      hbaseConf.set(key, value);
    }

    if (debug)
      HadoopConfigurationHelper.print(hbaseConf, "dump-hbaseConf-withProps.xml");

    // need to be in the titan.jar so that ServiceLoader.load() can load all FileSystem impls provided in the jar
    try (ClassLoaderContext cl = new ClassLoaderContext(FileSystem.class)) {
      ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
      for (FileSystem fs : serviceLoader) {
        final String fsImplKey = "fs." + fs.getScheme() + ".impl";
        if (props.getProperty(fsImplKey) == null) {
          log.debug("Setting Hadoop config property fs.{}.impl={}", fs.getScheme(), fs.getClass());
          hbaseConf.set(fsImplKey, fs.getClass().getName());
        } else {
          log.info("Not overriding manually set {} property with discovered {}", fsImplKey, fs.getClass().getName());
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
    log.debug("###########  {}", conf.get("fs.defaultFS"));
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
