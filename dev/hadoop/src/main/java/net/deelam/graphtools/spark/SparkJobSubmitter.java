/**
 * 
 */
package net.deelam.graphtools.spark;

import java.io.*;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import lombok.AllArgsConstructor;
import net.deelam.graphtools.spark.launcher.SparkLauncher;

/**
 * TODO: 5: improve spark job submission 
 * http://blog.sequenceiq.com/blog/2014/08/22/spark-submit-in-java/ 
 * vs. Spark 2.0 https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/launcher/package-summary.html
 */
public class SparkJobSubmitter {
  static Logger log = org.slf4j.LoggerFactory.getLogger(SparkJobSubmitter.class);

  final private SparkLauncher launcher; // calls spark-submit executable, thus requires SPARK_HOME be set

  public static void main(String[] args) throws IOException {
    URI uri = new File("lib").toURI();
    String filename = Paths.get(new File(".").getCanonicalFile().toURI()).relativize(Paths.get(uri)).toString();
    System.out.println(new File(".").getCanonicalFile().toURI() + " " + filename);
  }

  public SparkJobSubmitter(SparkJobConfig config) throws FileNotFoundException, IOException {
    { /// set env for child spark processes (executors?)
      Map<String, String> env = new HashMap<String, String>();
      log.warn("Skipping until needed: Setting environment of child spark process based on System.getProperties()",
          System.getProperties());
      if (false)
        for (Entry<Object, Object> e : System.getProperties().entrySet()) {
          log.warn("Skipping until needed: Propagating System.property: {}={}", e.getKey(), e.getValue());
          env.put(e.getKey().toString(), e.getValue().toString());
        }

      if (config.useHadoop()) {
        HadoopForSparkJobUtil.prepEnvForLauncher(config, env);
      }
      launcher = new SparkLauncher(env);
    }

    String sparkHome = findSparkHome(config);
    launcher.setSparkHome(sparkHome);

    if (config.useHadoop()) {
      HadoopForSparkJobUtil.configureLauncher(launcher, config);
    }

    launcher.setMaster(config.sparkMaster);
    if (config.deployMode != null)
      launcher.setDeployMode(config.deployMode); // e.g., "client" 

    launcher.setAppResource(config.appJar);
    launcher.setMainClass(config.mainClass);
    if(config.appArgs.size()>0)
      launcher.addAppArgs(config.appArgs.toArray(new String[config.appArgs.size()]));
    launcher.setConf("spark.app.name", config.appName);
    launcher.setVerbose(config.verbose);

    {
      @SuppressWarnings("unchecked")
      List<String> artifactFiles = config.staticProps.getList(SparkJobConfig.INPUT_FILES_TO_COPY);
      for (String file : artifactFiles) {
        Preconditions.checkState(new File(file).exists());
        launcher.addFile(file); // this copies the entire file to each executor
        // not for hdfs input files, which must be manually copied and set as an config.inputParam
      }
    }

    config.inputParams.forEach((key, value) -> {
      launcher.setConf("spark." + key, value);
    });

    if (config.classpath == null) {
      HadoopForSparkJobUtil.useHadoopClasspath(launcher);
    } else
      setClasspath(launcher, config);

    /**
     * Any values specified as flags or in the properties file will be passed on to the application and merged with those specified through SparkConf. 
     * Properties set directly on the SparkConf take highest precedence, then flags passed to spark-submit or spark-shell, 
     * then options in the spark-defaults.conf file
     */
    for (Iterator<String> itr = config.staticProps.getKeys(); itr.hasNext();) {
      String key = itr.next();
      List<String> valueList = config.staticProps.getList(key);
      if (valueList.size() > 1) {
        switch(key){
          case "spark.driver.extraJavaOptions":
          case "spark.executor.extraJavaOptions":
            String concatStr = valueList.stream().collect(Collectors.joining(" "));
            launcher.setConf(key, concatStr);
            break;
          //TODO: 2: handle other multivalued spark options
          default:
            log.error("What to do with this key {} with valueList={}", key, valueList);
        }
      } else {
        String value = config.staticProps.getString(key);
        if (key.startsWith("spark.")) {
          log.info("Setting conf {}={}", key, value);
          launcher.setConf(key, value);
        } else if (key.startsWith("env.")) {
          log.error("NEEDED?   Setting System property from " + key + " in spark-specific property file.");
          System.setProperty(key.substring("env.".length()), value);
        } else {
          log.warn(
              "Ignoring property {}={}  Launcher will only pass keys starting with 'spark.' to be set in SparkConf() of scala app.",
              key, value);
          break;
        }
      }
    }

  }

  static String findSparkHome(SparkJobConfig config) throws FileNotFoundException {
    String sparkHome = config.staticProps.getString("spark_home", null);
    if (sparkHome == null) {
      sparkHome = System.getenv().get("SPARK_HOME");
      if (sparkHome != null) {
        log.info("Using environment variable SPARK_HOME={}", sparkHome);
      } else {
        log.info("No environment variable SPARK_HOME; searching for spark directory...");
        String[] prefixes = {"spark_home", "spark-2", "spark-1", "spark-SKIP"};
        File sparkDirFile = findFirstDirectoryWithPrefix(new File("."), prefixes);
        if (sparkDirFile == null)
          throw new FileNotFoundException("SPARK_HOME directory not found with prefixes: " + Arrays.toString(prefixes));
        sparkHome = sparkDirFile.getAbsolutePath();
        log.info("Found directory to use as SPARK_HOME: {}", sparkHome);
      }
    }
    Preconditions.checkState(new File(sparkHome).exists(), "SPARK_HOME not found: " + sparkHome);
    return sparkHome;
  }

  /**
   * Possible optimization is to copy static jars to each machine in the cluster and use 'local:/path/to/file.jar'.
   * But that so cumbersome since cluster nodes can drop out and join in without our knowledge.
   * The impact is only significant if jars are large anyway. 
   */
  static void setClasspath(SparkLauncher launcher, SparkJobConfig config) {
    StringBuilder classpath = new StringBuilder(".");
    final Path currDirPath = Paths.get(new File("").toURI());
    config.classpath.forEach((fileKey, uri) -> {
      launcher.addJar(uri.toString()); // will make jars available (via Driver's http service or hdfs) to each executor's working directory

      String filename;
      // http://stackoverflow.com/questions/29972880/access-cassandra-from-spark-com-esotericsoftware-kryo-kryoexception-unable-to
      if (config.useHadoop()) {
        /** for 'yarn-cluster' spark.master mode, make sure the jar is included in launcher.addJar
         *  so that it's copied to the cluster, where the spark driver is running (even for hdfs: file paths) 
         *  AND that only the filename (not the path) is used because addJar copies(symlinks) jars to the working directory.
         *  http://spark.apache.org/docs/latest/running-on-yarn.html
         */
        filename = new File(fileKey).getName(); // complains about 'hdfs' provider: Paths.get(uri).getFileName().toString();
        if (!filename.equals(uri))
          log.info("For 'yarn-cluster' mode, changing classpath from '{}' to '{}'", uri, filename);
      } else {
        File file = new File(uri);
        if (!file.exists()) {
          log.warn("File not found: {}", uri);
        }
        filename = currDirPath.relativize(Paths.get(uri)).toString();
        //filename=file.getAbsolutePath(); //uri.toString();
      }
      // spark.driver.extraClassPath and spark.executor.extraClassPath must use colons
      classpath.append(":").append(filename);
    });

    /**
     * http://stackoverflow.com/questions/37132559/add-jars-to-a-spark-job-spark-submit
     * - spark.driver.extraClassPath or it's alias --driver-class-path to set extra classpaths on the Master node.
     * - spark.executor.extraClassPath to set extra class path on the Worker nodes.
     * If you want a certain JAR to be effected on both the Master and the Worker, you have to specify these separately in BOTH flags.
     * Use colon separator on Linux.
     *
     * --conf spark.driver.extraClassPath=... or --driver-class-path: These are aliases, doesn't matter which one you choose
     * --conf spark.driver.extraLibraryPath=..., or --driver-library-path ... Same as above, aliases.
     *  
     * --conf spark.executor.extraClassPath=...: Use this when you have a dependency which can't be included in an uber JAR (for example, because there are compile time conflicts between library versions) and which you need to load at runtime.
     * --conf spark.executor.extraLibraryPath=... This is passed as the java.library.path option for the JVM. Use this when you need a library path visible to the JVM.        
     */
    String classpathStr = classpath.toString();

    log.debug(SparkLauncher.DRIVER_EXTRA_CLASSPATH); // "spark.driver.extraClassPath":
    // Usually do not use launcher.addSparkArg() as it clears out lists before setting the value
    // However, launcher.setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH,)  
    // does not put jars before spark-assembly.jar:  launcher.setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, classpathStr);
    // Use addSparkArgs("--driver-class-path",) to put jars before spark-assembly.jar (esp. for master="local")
    launcher.addSparkArg("--driver-class-path", classpathStr);
    
    log.debug(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH); //"spark.executor.extraClassPath"
    launcher.setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, classpathStr);
  }



  private static FileFilter directoryFilter = (File dir) -> dir.isDirectory();

  public static File findFirstDirectoryWithPrefix(final File parentDir, final String[] dirPrefixes) {
    final File[] fileList = parentDir.listFiles(directoryFilter);
    for (String prefix : dirPrefixes) {
      for (File f : fileList)
        if (f.getName().startsWith(prefix))
          return f;
    }
    return null;
  }

  /// ----------------------------------------------------------------------

  public Process startJob(Consumer<SparkLauncher> configure) throws IOException {
    configure.accept(launcher);
    Process spark = launcher.launch();
    // redirect spark process output
    StreamGobbler errorGobbler = new StreamGobbler(spark.getErrorStream(), StreamGobbler.StreamLogLevel.ERR);
    StreamGobbler outputGobbler = new StreamGobbler(spark.getInputStream(), StreamGobbler.StreamLogLevel.OUT);
    errorGobbler.start();
    outputGobbler.start();
    return spark;
  }

  // http://stackoverflow.com/questions/1732455/redirect-process-output-to-stdout
  @AllArgsConstructor
  static class StreamGobbler extends Thread {
    private static final Logger logLauncher = LoggerFactory.getLogger(StreamGobbler.class);
    InputStream is;
    StreamLogLevel logLevel;

    enum StreamLogLevel {
      OUT, ERR
    };

    public void run() {
      try (InputStreamReader isr = new InputStreamReader(is, Charset.forName("UTF-8"))) {
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while ((line = br.readLine()) != null) {
          switch (logLevel) {
            case ERR: // all output seems to go to ERR
              logLauncher.info(line);
              break;
            case OUT:
              logLauncher.info(line);
              break;
          }
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }
  }
}
