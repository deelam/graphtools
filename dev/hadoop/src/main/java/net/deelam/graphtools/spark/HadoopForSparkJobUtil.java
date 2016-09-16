package net.deelam.graphtools.spark;

import java.io.File;
import java.util.Map;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.spark.launcher.SparkLauncher;

@Slf4j
final class HadoopForSparkJobUtil {
  static final String HADOOP_PROPERTY_PREFIX = "spark.hadoop.";

  static void prepEnvForLauncher(SparkJobConfig config, Map<String, String> env) {
    if (config.htConfigs == null)
      throw new IllegalArgumentException("config.htConfigs is null!");

    setHadoopUserName(config, env);

    // check settings
    {
      String yarnConfDir = env.get("YARN_CONF_DIR");
      if (yarnConfDir == null) {
        yarnConfDir = config.htConfigs.getHadoopConfig().get("YARN_CONF_DIR");
        log.info("Inferring YARN_CONF_DIR from hadoop config: {}", yarnConfDir);
        env.put("YARN_CONF_DIR", yarnConfDir);
      }
      if (yarnConfDir != null)
        Preconditions.checkState(new File(yarnConfDir).exists(),
            "YARN_CONF_DIR doesn't exists: " + env.get("YARN_CONF_DIR"));
    }
  }

  static void setHadoopUserName(SparkJobConfig config, Map<String, String> env) {
    String hadoopUser = config.htConfigs.getHadoopConfig().get("env.HADOOP_USER_NAME");
    if (hadoopUser != null) { // may be overriden by env.HADOOP_USER_NAME from Hadoop property file
      log.info("Using config.htConfigs's env.HADOOP_USER_NAME={}", hadoopUser);
    } else {
      hadoopUser = System.getProperties().getProperty("HADOOP_USER_NAME");
      if (hadoopUser != null)
        log.info("Using System property HADOOP_USER_NAME={}", hadoopUser);
      else {
        hadoopUser = System.getenv().get("HADOOP_USER_NAME");
        if (hadoopUser != null)
          log.info("Using environment variable HADOOP_USER_NAME={}", hadoopUser);
      }
    }
    if (hadoopUser == null) {
      log.warn("Could not set HADOOP_USER_NAME for SparkLauncher's env, which is required for HDFS access");
    } else {
      env.put("HADOOP_USER_NAME", hadoopUser);
    }
  }

  static  void configureLauncher(SparkLauncher launcher, SparkJobConfig config) {
    log.warn("Skipping until needed (only needed for spark jobs that use hadoop): Setting hadoop properties");
    if(false) config.htConfigs.getHadoopConfig().forEach(entry -> {
      // setConf() for setting SparkConf in app vs. env.set() for child's System.getProperty()
      launcher.setConf(HADOOP_PROPERTY_PREFIX+entry.getKey(), entry.getValue());
      log.info("Setting hadoop property: {}={}", HADOOP_PROPERTY_PREFIX + entry.getKey(), entry.getValue());
    });
  }

  static void useHadoopClasspath(SparkLauncher launcher) {
    String hadoopJobClasspath = null; //mapReduceProps.getProperty(GetConfigurations.TOOLRUNNER_LIBARGS, null);
    if (hadoopJobClasspath == null) {
      log.error("Cannot determine job's classpath");
    } else { // This is usually this case
      log.info("Using hadoop job's classpath for spark: {}", hadoopJobClasspath);
      String[] jarFiles = hadoopJobClasspath.split(",");
      for (String jarF : jarFiles) {
        jarF = jarF.trim();
        if (!jarF.startsWith("hdfs:"))
          Preconditions.checkState(new File(jarF).exists(), "Could not find file: " + jarF);
        launcher.addJar(jarF);
      }
    }
  }
  
}