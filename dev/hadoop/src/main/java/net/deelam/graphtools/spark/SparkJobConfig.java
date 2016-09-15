package net.deelam.graphtools.spark;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.hadoop.HadoopConfigs;
import net.deelam.graphtools.hadoop.HadoopTitanConfigs;
import net.deelam.graphtools.hadoop.HdfsUtils;

@ToString
@Accessors(chain = true, fluent = true)
@Slf4j
//@Getter
public class SparkJobConfig {
  public static final String YARN_CLUSTER = "yarn-cluster"; //possible value for sparkMaster
  public static final String LOCAL = "local[*]"; //possible value for sparkMaster
  String sparkMaster = LOCAL;
  String appNamePrefix;
  String appJar;
  String mainClass;

  Configuration config;
  Map<String, String> classpath = new LinkedHashMap<>(); // must preserve order
  Map<String, String> filesForJob = new HashMap<>();
  Map<String, Object> inputParams = new HashMap<>();
  Map<String, Object> staticProps = new HashMap<>();

  //  boolean loadHtConfigs = false;
  HadoopConfigs htConfigs = null;

  public boolean isReady() {
    String[] fields = {sparkMaster, appNamePrefix, appJar, mainClass};
    for (int i = 0; i < fields.length; ++i)
      if(fields[i]==null || fields[i].isEmpty()){
        log.warn("Field {} is empty!", i);
        return false;
      }

    Map[] maps = {classpath, filesForJob};
    for (int i = 0; i < maps.length; ++i)
      if (maps[i].isEmpty()) {
        log.warn("Map {} is empty!", i);
        return false;
      }

    AtomicBoolean isReady = new AtomicBoolean(true);
    getRequiredInputParams().forEach(p->{
      if(!inputParams.containsKey(p)){
        log.warn("Required parameter not set: {}", p);
        isReady.set(false);
      }
    });

    return isReady.get();
  }

  public List<String> getRequiredInputParams(){
    @SuppressWarnings("unchecked")
    List<String> reqInput = config.getList("inputParams.required");
    return reqInput;
  }
  
  public List<String> getOptionalInputParams(){
    @SuppressWarnings("unchecked")
    List<String> reqInput = config.getList("inputParams.optional");
    return reqInput;
  }

  public void setInputParams(String p, Object val) {
    inputParams.put(p, val);
  }

  @SuppressWarnings("unchecked")
  public static SparkJobConfig read(String filename) throws ConfigurationException {
    SparkJobConfig sj = new SparkJobConfig();
    Configuration config = new PropertiesConfiguration(filename);
    sj.config = config;

    sj.appNamePrefix = config.getString("appNamePrefix", filename);
    sj.mainClass = config.getString("mainClass");
    sj.appJar = config.getString("appJar");

//    reqInput.stream().forEach(key -> sj.inputParamsRequired.put(key, Void.TYPE));

//    List<String> optInput = config.getList("inputParams.optional");
//    optInput.stream().forEach(key -> sj.inputParamsOptional.put(key, Optional.empty()));

    //config.getKeys("spark").forEachRemaining(System.out::println);
    ((Iterator<String>) config.getKeys("spark"))
        .forEachRemaining(key -> sj.staticProps.put(key, config.getString(key)));

    return sj;
  }

  public SparkJobConfig setSparkMaster(String masterMode, HadoopConfigs htConfigs) {
    sparkMaster = masterMode;
    this.htConfigs = htConfigs;
    if (YARN_CLUSTER.equals(sparkMaster)) {
      htConfigs.loadConfigs();
    }
    return this;
  }

  @SuppressWarnings("unchecked")
  public void resolveFiles() {
    resolveLocalFiles(config.getList("files"), filesForJob);
  }

  @SuppressWarnings("unchecked")
  public void resolveClasspath() {
    resolveLocalFiles(config.getList("classpath"), classpath);
  }

  @SuppressWarnings("unchecked")
  public void copyFilesToHdfs(URI destDirUri, boolean overwrite) {
    copyFilesToHdfs(overwrite, destDirUri, config.getList("files"), filesForJob);
  }

  @SuppressWarnings("unchecked")
  public void copyClasspathToHdfs(URI destDirUri, boolean overwrite) {
    copyFilesToHdfs(overwrite, destDirUri, config.getList("classpath"), classpath);
  }

  private void resolveLocalFiles(List<String> files, Map<String, String> map) {
    files.stream().forEach(srcFile -> {
      File localFile = new File(srcFile);
      map.put(srcFile, localFile.toURI().toString());
    });
  }

  private static Map<String, String> hdfsFileCache = new HashMap<String, String>();

  public void clearHdfsCache() {
    hdfsFileCache.clear();
  }

  private void copyFilesToHdfs(boolean overwrite, URI destDirUri, List<String> files, Map<String, String> map) {
    HdfsUtils hdfs = htConfigs.getHdfsUtils();
    org.apache.hadoop.fs.Path destDir = new org.apache.hadoop.fs.Path(destDirUri);
    files.stream().forEach(srcFile -> {
      File localFile = new File(srcFile);
      try {
        String hdfsPath = hdfsFileCache.get(srcFile);
        boolean exists = false;
        if (hdfsPath != null)
          exists = hdfs.exists(hdfsPath);
        if (!exists) {
          org.apache.hadoop.fs.Path dstPath;
          try {
            dstPath = hdfs.uploadFile(localFile, destDir, overwrite);
          } catch (FileAlreadyExistsException e) {// can occur when overwrite=false
            dstPath = new org.apache.hadoop.fs.Path(hdfs.makeQualified(destDir), localFile.getName());
            log.warn("Not overwritting existing file: {}", dstPath);
          }
          map.put(srcFile, dstPath.toString());
          hdfsFileCache.put(srcFile, dstPath.toString());
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

}
