package net.deelam.osgientry.internal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;

public class StandaloneWebapp {

  public static void main(String[] argv) throws Exception {

    if (System.getProperty("log4j.configurationFile") == null && new File("./log4j.xml").exists()) {
      // set a default if file exist
      System.out.println("Found ./log4j.xml file.  Setting log4j.configurationFile to it.");
      System.setProperty("log4j.configurationFile", "./log4j.xml");
    }
    checkForLog4jFile();

    try {
      Map<String, String> config = new HashMap<String, String>();
      config.put(Constants.FRAMEWORK_STORAGE_CLEAN, Constants.FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT);
      config.put("osgi.noShutdown", "true");
      config.put("org.osgi.service.http.port", "8380");
      config.put("org.osgi.service.http.port.secure", "8383");
      config.put("org.apache.felix.https.keystore", "testkeystore");
      config.put("org.apache.felix.https.keystore.password", "testing");
      config.put("org.apache.felix.https.enable", "true");
      config.put("org.apache.felix.http.jettyEnabled", "true"); // needed when using the all-in-one felix-http-bundle

      config.put("org.osgi.framework.system.packages.extra",
          "sun.misc," // sun.misc.Unsafe is required when running Hadoop
              + "com.sun.jdi,com.sun.jdi.connect,com.sun.jdi.event,com.sun.jdi.request,"
              + "com.sun.xml.internal.stream,"
              + "sun.nio,sun.nio.ch");

      // allows default configs to be overwritten from a file
      String propFile = "osgi.ini";
      try {
        Properties properties = OsgiUtils.loadProperties(propFile, new Properties());
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
          config.put(entry.getKey().toString(), entry.getValue().toString());
        }
      } catch (FileNotFoundException e) {
        System.out.println("Using defaults since property file doesn't exist: " + propFile);
      }

      final String httpEnabledStr = config.getOrDefault("org.osgi.service.http.enabled", "true");
      boolean httpEnabled = Boolean.parseBoolean(httpEnabledStr);
      if (httpEnabled) {
        final String port = config.get("org.osgi.service.http.port");
        final String sslPort = config.get("org.osgi.service.http.port.secure");

        // Check if port is already in use
        if (isPortInUse("localhost", Integer.parseInt(port))) {
          System.err.println("Port is already in use: " + port);
          throw new java.net.BindException("Port is already in use: " + port);
        }
        if (isPortInUse("localhost", Integer.parseInt(sslPort))) {
          System.err.println("Port is already in use: " + port);
          throw new java.net.BindException("Port is already in use: " + port);
        }

        // Print welcome banner.
        System.out.println("\n========  Starting application on port " + port +
            " and secure port " + sslPort + " ===========");
        System.out.println("   If enabled, Felix WebConsole is available at http://localhost:" +
            port + "/system/console. \n");

        // set trustStore and keyStore info based on propFile if not already set (via commandline argument 'java -Dprop=val')
        for (String key : trustStorePropertyKeys) {
          String val = config.get(key);
          if (val == null)
            continue;
          String existingVal = System.getProperty(key);
          if (existingVal != null) {
            System.out.println(" System property already set: " + key + "  Ignoring property specified in " + propFile);
          } else {
            if (key.contains("ssword"))
              System.out.println("  Setting system property: " + key + " specified in " + propFile);
            else
              System.out.println("  Setting system property: " + key + " = " + val + " specified in " + propFile);
            System.setProperty(key, val);
          }
        }

        String requireClientKeystoreStr=config.getOrDefault("requireClientKeystore", "false");
        boolean requireClientKeystore=Boolean.parseBoolean(requireClientKeystoreStr);
        checkForTrustStore(requireClientKeystore);
      } else {
        System.out.println("\n========  Starting application ===========");
      }

      //System.out.println("CONFIG: "+config);

      //System.err.println(config.get("org.osgi.framework.system.packages.extra"));
      final Framework framework = getFrameworkFactory().newFramework(config);
      framework.init();
      //AutoProcessor.process(null, m_fwk.getBundleContext());
      framework.start();
      installAndStartBundles(framework.getBundleContext(), "1-support");

      System.out.println("Adding OSGi shutdown hook");
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        System.out.println("Shutdown hook invoked, stopping OSGi Framework.");
        try {
          framework.stop();
          System.out.println("Waiting up to 5s for OSGi shutdown to complete... "
              + StandaloneWebapp.class.getSimpleName());
          framework.waitForStop(5000);
        } catch (Exception e) {
          System.err.println("Failed to cleanly shutdown OSGi Framework: " + e.getMessage());
          e.printStackTrace();
        }
      }));

      framework.waitForStop(0);
      System.exit(0);
    } catch (Exception ex) {
      System.err.println("Could not create framework: " + ex);
      ex.printStackTrace();
      System.exit(-1);
    }
  }

  private static boolean isPortInUse(String hostName, int portNumber) {
    try {
      Socket s = new Socket(hostName, portNumber);
      s.close();
      return true;
    } catch (Exception e) {
      return false;
    }
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

  private static final String[] trustStorePropertyKeys = {
      "javax.net.ssl.trustStore",
      "javax.net.ssl.trustStorePassword",
      "javax.net.ssl.keyStore",
      "javax.net.ssl.keyStorePassword"};

  private static void checkForTrustStore(boolean requireClientKeystore) {
    Map<String, String> env = System.getenv();
    for (String key : trustStorePropertyKeys) {
      String val = System.getProperty(key);
      if (val == null) {
        //System.err.println(" Java system property '"+key+"' not set!");
        String envVal;
        if (key.contains("ssword"))
          envVal = env.get("KEYSTORE_PWD");
        else
          envVal = env.get("KEYSTORE_FILE");
        if (envVal == null) {
          if(requireClientKeystore){
            System.err.println("  Please set and export KEYSTORE_FILE and KEYSTORE_PWD environment variables!!");
            System.exit(101);
          }else{
            System.out.println("  KEYSTORE_FILE and KEYSTORE_PWD environment variables not set; not setting client-side keystore.");
          }
        } else {
          if (key.contains("ssword"))
            System.out.println("  Setting system property: " + key + " from environment variable");
          else
            System.out.println("  Setting system property: " + key + " = " + envVal + " from environment variable");
          System.setProperty(key, envVal);
        }
      }
    }
  }

  private static FrameworkFactory getFrameworkFactory() throws Exception {
    java.net.URL url = StandaloneWebapp.class.getClassLoader().getResource(
        "META-INF/services/org.osgi.framework.launch.FrameworkFactory");
    if (url != null) {
      BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
      try {
        for (String s = br.readLine(); s != null; s = br.readLine()) {
          s = s.trim();
          // Try to load first non-empty, non-commented line.
          if ((s.length() > 0) && (s.charAt(0) != '#')) {
            return (FrameworkFactory) Class.forName(s).newInstance();
          }
        }
      } finally {
        if (br != null)
          br.close();
      }
    }
    throw new Exception("Could not find framework factory.");
  }

  static private void installAndStartBundles(BundleContext context, String dir) throws Exception {
    ArrayList<Bundle> installed = new ArrayList<Bundle>();
    for (URL url : findBundles(dir)) {
      System.out.println("Installing bundle : " + url.toExternalForm());
      Bundle bundle = context.installBundle(url.toExternalForm());
      installed.add(bundle);
    }
    for (Bundle bundle : installed) {
      System.out.println("  * Starting bundle : " + bundle);
      bundle.start();
    }
  }

  static private List<URL> findBundles(String dir) throws Exception {
    ArrayList<URL> list = new ArrayList<URL>();
    File dirFile = new File(dir);
    if (dirFile.exists()) {
      final File[] files = dirFile.listFiles();
      if (files != null) {
        URL autoloadJar = null;
        Arrays.sort(files);
        for (File file : files) {
          final String filename = file.getName();
          if (filename.endsWith(".jar")) {
            URL url = file.toURI().toURL();
            if (filename.contains("arlut.osgi.autoloader")) {
              System.out.println("found jar: " + url + " (reordering autoloader to be loaded last)");
              autoloadJar = url;
            } else {
              System.out.println("found jar: " + url);
              list.add(url);
            }
          }
        }
        if (autoloadJar != null) {
          list.add(autoloadJar);
        }
      }
    }
    return list;
  }
}
