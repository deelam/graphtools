package net.deelam.graphtools.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PropertiesUtils {
  public static void loadProperties(String filename, Properties properties) throws FileNotFoundException, IOException {
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
      log.info("Loading property file: "+ propFile.getAbsolutePath());
      try (InputStreamReader reader = new InputStreamReader(new FileInputStream(propFile), "UTF-8")) {
        properties.load(reader);
      }
      String includedFiles=properties.getProperty("include");
      if (includedFiles != null){
        log.info("  Also loading includedFiles={}", includedFiles);
        for (String includedFile : includedFiles.split(",")) {
          Properties props = new Properties();
          loadProperties(includedFile.trim(), props);
          for (String key : props.stringPropertyNames())
            if (properties.containsKey(key))
              log.info("Ignoring included "+key+"="+props.getProperty(key)+
                  ", using value '"+properties.getProperty(key)+"' instead");
            else
              properties.put(key, props.getProperty(key));
        }
      }
    } else {
      throw new FileNotFoundException("Property file doesn't exist: " + propFile.getAbsolutePath());
    }
  }
  
}

