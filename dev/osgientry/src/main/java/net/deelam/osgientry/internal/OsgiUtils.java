package net.deelam.osgientry.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;
import java.util.jar.Manifest;

public class OsgiUtils {
  
  public static String getBuildNumber(ClassLoader clOfBundle) throws IOException {
    // Check Manifest file for generated build number
    Manifest mf = new Manifest();
    InputStream is = clOfBundle.getResourceAsStream("META-INF/MANIFEST.MF");
    if (is == null) {
      return "(No META-INF/MANIFEST.MF file)";
    }
    mf.read(is);
    return mf.getMainAttributes().getValue("BuildNumber");
  }
  
  public static Properties loadProperties(String filename, Properties properties) throws FileNotFoundException, IOException {
    File propFile=new File(filename);
    if (!propFile.exists()) { // then look in classpath
      //System.out.println("Searching for property file in classpath: "+filename);
      URL url = Runtime.class.getResource("/"+filename);
      if(url!=null)
        try {
          propFile=new File(url.toURI());
        } catch (URISyntaxException e) {
        }
    }
    if (propFile.exists()) {
      System.out.println("Loading property file: "+ propFile.getAbsolutePath());
      try (InputStreamReader reader = new InputStreamReader(new FileInputStream(propFile), "UTF-8")) {
        properties.load(reader);
      }
      String includedFiles=properties.getProperty("include");
      if (includedFiles != null)
        for (String includedFile : includedFiles.split(",")) {
          Properties props = loadProperties(includedFile.trim(), new Properties());
          for (String key : props.stringPropertyNames())
            if (properties.containsKey(key))
              System.out.println("Ignoring included "+key+"="+props.getProperty(key)+
                  ", using value '"+properties.getProperty(key)+"' instead");
            else
              properties.put(key, props.getProperty(key));
        }
      return properties;
    } else {
      throw new FileNotFoundException("Property file doesn't exist: " + propFile.getAbsolutePath());
    }
  }
  
}

