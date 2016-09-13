package net.deelam.common.util;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

public class Log4jUtil {
  
  public static void loadXml(){
    if (System.getProperty("log4j.configurationFile") == null && new File("./log4j.xml").exists()) {
      // set a default if file exist
      System.out.println("Found ./log4j.xml file in current directory.  Setting log4j.configurationFile to it.");
      System.setProperty("log4j.configurationFile", "./log4j.xml");
    }
    checkForLog4jFile();
  }
  private static void checkForLog4jFile() {
    String logConfigFile = System.getProperty("log4j.configurationFile");
    if (logConfigFile != null) { // if property set, then check that file can be found
      System.out.println("  Checking System property: log4j.configurationFile=" + logConfigFile);
      ClassLoader cl = ClassLoader.getSystemClassLoader();
      if (cl.getResource(logConfigFile) == null) {
        URL[] urls = ((URLClassLoader) cl).getURLs();
        System.err.println("  !!! Could not find logging config file " + logConfigFile
            + " in classpath: " + Arrays.toString(urls));
      } else {
        System.out.println("    Found in classpath: " + logConfigFile);
      }
    }
  }
}
