package net.deelam.common.util;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

public class Log4jUtil {
  
  private static final String LOG4J_CONFIGURATION_FILE = "log4j.configurationFile";
  
  public static void loadXml(){
    File file = new File("log4j.xml");
    if (System.getProperty(LOG4J_CONFIGURATION_FILE) == null && file.exists()) {
      // set a default if file exist
      System.out.println("Found log4j.xml file in current directory.  Setting log4j.configurationFile=log4j.xml");
      System.setProperty(LOG4J_CONFIGURATION_FILE, "log4j.xml");
    }
    checkForLog4jFile();
  }
  private static void checkForLog4jFile() {
    String logConfigFile = System.getProperty(LOG4J_CONFIGURATION_FILE);
    if (logConfigFile != null) { // if property set, then check that file can be found
      //System.out.println("  Checking that System property: log4j.configurationFile=" + logConfigFile+" is in classpath");
      {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        if (cl.getResource(logConfigFile) == null) {
          URL[] urls = ((URLClassLoader) cl).getURLs();
          System.err.println("  !!! Could not find logging config file " + logConfigFile
              + " in classpath: " + Arrays.toString(urls));
          System.err.println("Remember to add path to log4j.xml file to classpath!");
        } else {
          System.out.println("  Found in classpath: " + logConfigFile);
        }
      }
    }
  }
}
