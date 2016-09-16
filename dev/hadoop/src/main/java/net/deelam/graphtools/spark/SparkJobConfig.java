package net.deelam.graphtools.spark;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import net.deelam.common.util.ManifestUtil;
import net.deelam.graphtools.hadoop.HadoopConfigs;
import net.deelam.graphtools.hadoop.HdfsUtils;

@ToString
@Accessors(chain = true)
@Slf4j
//@Getter
public class SparkJobConfig {
  public static final String LOCAL = "local[*]"; // for local execution 
  public static final String YARN_CLUSTER = "yarn-cluster"; // spark driver runs on yarn; logs located on yarn
  public static final String YARN_CLIENT = "yarn-client"; // good for debugging on yarn since we see logs on local client (except for some Hadoop IOExceptions)

  Configuration config;
  String sparkMaster = LOCAL;
  String deployMode;
  String appNamePrefix;
  @Getter
  @Setter
  String appJar;
  String mainClass;
  @Getter
  @Setter
  List<String> appArgs;
  boolean verbose = true; // verbosity of spark-submit script
  String log4jFile; // affects application logging
  Configuration staticProps = new BaseConfiguration();

  HadoopConfigs htConfigs = null;

  @Getter
  String appName;
  Map<String, URI> classpath = new LinkedHashMap<>(); // must preserve order
  Map<String, URI> staticFilesForJob = new HashMap<>();
  Map<String, String> inputParams = new HashMap<>();

  public boolean useHadoop() {
    return sparkMaster.startsWith("yarn");
  }

  public boolean isReady() {
    String[] fields = {sparkMaster, appNamePrefix, appJar, mainClass, appName};
    for (int i = 0; i < fields.length; ++i)
      if (fields[i] == null || fields[i].isEmpty()) {
        log.warn("Field {} is empty!", i);
        return false;
      }

    Map<?, ?>[] maps = {classpath, staticFilesForJob};
    for (int i = 0; i < maps.length; ++i)
      if (maps[i].isEmpty()) {
        log.info("Map {} is empty!", i);
        //okay if empty: return false;
      }

    String[] filenames = {appJar};
    for (int i = 0; i < filenames.length; ++i) {
      if (!new File(filenames[i]).exists()) {
        log.warn("File {} does not exist!", filenames[i]);
        return false;
      }
    }

    AtomicBoolean isReady = new AtomicBoolean(true);
    getRequiredInputParams().forEach(p -> {
      if (!inputParams.containsKey(p)) {
        log.warn("Required parameter not set: {}", p);
        isReady.set(false);
      }
    });

    return isReady.get();
  }

  public List<String> getRequiredInputParams() {
    @SuppressWarnings("unchecked")
    List<String> reqInput = config.getList("inputParams.required");
    return reqInput;
  }

  public List<String> getOptionalInputParams() {
    @SuppressWarnings("unchecked")
    List<String> reqInput = config.getList("inputParams.optional");
    return reqInput;
  }

  public void setInputParams(String key, String val) {
    inputParams.put(key, val);
  }

  public String getInputParam(String key) {
    return inputParams.get(key);
  }

  public void setLog4j(String log4jFile) {
    this.log4jFile = log4jFile;
    if (log4jFile == null)
      return;
    if (!new File(log4jFile).exists())
      log.warn("Log4j file not found: {}", log4jFile);
    else {
      addToFileArtifacts(log4jFile);
      staticProps.addProperty("spark.driver.extraJavaOptions", "-Dlog4j.configuration=" + log4jFile); // so log4j will load file
    }
  }

  static final String INPUT_FILES_TO_COPY = "spark.file.artifacts";

  // so it can be copied to each spark executors
  public void addToFileArtifacts(String requiredFile) {
    staticProps.addProperty(INPUT_FILES_TO_COPY, requiredFile);
  }

  public org.apache.hadoop.fs.Path copyToHdfs(HadoopConfigs htConfigs, String localFile, String destDirStr,
      boolean overwrite) throws IOException {
    HdfsUtils hdfs = htConfigs.getHdfsUtils();
    org.apache.hadoop.fs.Path destDir = hdfs.ensureDirExists(destDirStr); //new org.apache.hadoop.fs.Path(destDirUri);
    org.apache.hadoop.fs.Path dstPath = hdfs.uploadFile(new File(localFile), destDir, overwrite);
    return dstPath;
  }

  @SuppressWarnings("unchecked")
  public static SparkJobConfig read(String filename) throws ConfigurationException {
    SparkJobConfig sj = new SparkJobConfig();
    Configuration config = new PropertiesConfiguration(filename);
    sj.config = config;

    int dotIndex = filename.lastIndexOf('.');
    String filebasename = filename;
    if (dotIndex > 1)
      filebasename = filename.substring(0, dotIndex);

    sj.appNamePrefix = config.getString("appNamePrefix", filebasename);
    long uniqSuffix = System.currentTimeMillis();
    sj.appName = config.getString("appName", sj.appNamePrefix + uniqSuffix);
    sj.mainClass = config.getString("mainClass");
    sj.appJar = config.getString("appJar");
    sj.appArgs = config.getList("appArgs", new ArrayList<String>());

    // spark-submitter script verbosity
    sj.verbose = config.getBoolean("verbose", sj.verbose);

    // app logging
    sj.log4jFile = config.getString("log4jFile", filebasename + "-log4j.xml");

    //    reqInput.stream().forEach(key -> sj.inputParamsRequired.put(key, Void.TYPE));

    //    List<String> optInput = config.getList("inputParams.optional");
    //    optInput.stream().forEach(key -> sj.inputParamsOptional.put(key, Optional.empty()));

    //config.getKeys("spark").forEachRemaining(System.out::println);
    ((Iterator<String>) config.getKeys("spark"))
        .forEachRemaining(key -> sj.staticProps.addProperty(key, config.getString(key)));

    return sj;
  }

//  public static String DEFAULT_DEST_DIR = "/tmp/sparkjobfiles/";
//
//  public SparkJobConfig setSparkMaster(String master, HadoopConfigs htConfigs) throws IOException {
//    URI destDirUri = null;
//    if (YARN_CLUSTER.equals(master)) {
//      destDirUri = URI.create(DEFAULT_DEST_DIR);
//    }
//    return setSparkMaster(master, null, htConfigs, destDirUri, false);
//  }

  /**
   * 
   * @param master
   * @param deployMode "client" or "cluster"
   * @param htConfigs
   * @param destDirUri location for artifact files and classpath jars
   * @param overwrite
   * @return
   * @throws IOException
   */
  public SparkJobConfig setSparkMaster(String master, String deployMode, HadoopConfigs htConfigs, URI destDirUri,
      boolean overwrite) throws IOException {
    sparkMaster = master;
    this.deployMode = deployMode;
    this.htConfigs = htConfigs;
    if (sparkMaster.startsWith("yarn")) {
      htConfigs.loadConfigs();
      copyFilesToHdfs(destDirUri, overwrite);
      copyClasspathToHdfs(destDirUri, overwrite);
    } else if (sparkMaster.startsWith("local")) {
      // if hdfs desired, call copyFilesToHdfs() and copyClasspathToHdfs() after this
      resolveFiles();
      resolveClasspath();
    } else {
      throw new UnsupportedOperationException(sparkMaster);
    }

    /// set other confs
    setLog4j(log4jFile);

    return this;
  }

  @SuppressWarnings("unchecked")
  public void resolveFiles() {
    resolveLocalFiles(config.getList("files"), staticFilesForJob);
  }

  public void resolveClasspath() throws IOException {
    List<String> list = getOrInferClasspath();
    resolveLocalFiles(list, classpath);
  }

  @SuppressWarnings("unchecked")
  private List<String> getOrInferClasspath() throws IOException {
    String cpStr = config.getString("classpath");
    List<String> list;
    if (cpStr.equalsIgnoreCase("INFER_FROM_APPJAR"))
      list = ManifestUtil.getClasspathJars(appJar);
    else
      list = config.getList("classpath");
    return list;
  }

  @SuppressWarnings("unchecked")
  public void copyFilesToHdfs(URI destDirUri, boolean overwrite) throws IOException {
    copyFilesToHdfs(overwrite, destDirUri, config.getList("files"), staticFilesForJob);
  }

  public void copyClasspathToHdfs(URI destDirUri, boolean overwrite) throws IOException {
    List<String> list = getOrInferClasspath();
    copyFilesToHdfs(overwrite, destDirUri, list, classpath);
  }

  private void resolveLocalFiles(List<String> files, Map<String, URI> map) {
    files.stream().forEach(srcFile -> {
      File localFile = new File(srcFile);
      URI existing = map.put(srcFile, localFile.toURI());
      if (existing != null)
        log.warn("Overriding existing {}={} with {}", srcFile, existing, localFile.toURI());
    });
  }

  private static Map<String, URI> hdfsFileCache = new HashMap<>();

  public void clearHdfsCache() {
    hdfsFileCache.clear();
  }

  private void copyFilesToHdfs(boolean overwrite, URI destDirUri, List<String> files, Map<String, URI> map)
      throws IOException {
    HdfsUtils hdfs = htConfigs.getHdfsUtils();
    org.apache.hadoop.fs.Path destDir = hdfs.ensureDirExists(destDirUri.toString()); //new org.apache.hadoop.fs.Path(destDirUri);
    files.stream().forEach(srcFile -> {
      File localFile = new File(srcFile);
      try {
        URI hdfsPath = hdfsFileCache.get(srcFile);
        boolean exists = false;
        if (hdfsPath != null)
          exists = hdfs.exists(hdfsPath.toString());
        if (!exists) {
          org.apache.hadoop.fs.Path dstPath;
          try {
            dstPath = hdfs.uploadFile(localFile, destDir, overwrite);
          } catch (FileAlreadyExistsException e) {// can occur when overwrite=false
            dstPath = new org.apache.hadoop.fs.Path(hdfs.makeQualified(destDir), localFile.getName());
            log.warn("Not overwritting existing file: {}", dstPath);
          }
          //if(map!=null)
          {
            URI existing = map.put(srcFile, dstPath.toUri());
            if (existing != null)
              log.warn("Overriding existing {}={} with {}", srcFile, existing, localFile.toURI());
          }

          hdfsFileCache.put(srcFile, dstPath.toUri());
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

}
