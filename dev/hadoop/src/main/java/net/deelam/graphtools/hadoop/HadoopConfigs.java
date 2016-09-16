package net.deelam.graphtools.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class HadoopConfigs {
  
  public HadoopConfigs copy(){
    HadoopConfigs copy=new HadoopConfigs(hadoopPropsFile);
    return copy(copy);
  }
  
  protected HadoopConfigs copy(HadoopConfigs copy){
    if(hadoopConfig!=null) 
      copy.hadoopConfig=new org.apache.hadoop.conf.Configuration(hadoopConfig);
    if(minHadoopConfig!=null) 
      copy.minHadoopConfig=new org.apache.hadoop.conf.Configuration(minHadoopConfig);
    return copy;
  }
  
  final String hadoopPropsFile;
  
  public HadoopConfigs(){
    this(null);
  }
  
  private HadoopConfigurationHelper helper = new HadoopConfigurationHelper();

  public void loadConfigs(){
    getHadoopConfig();
  }
  
  public org.apache.hadoop.conf.Configuration getHadoopConfig() {
    try {
      return getMinimalHadoopConfig();
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    } 
  }
  
  private org.apache.hadoop.conf.Configuration hadoopConfig = null;
  org.apache.hadoop.conf.Configuration getAllHadoopConfig() throws ConfigurationException{
    if (hadoopConfig == null) {
      hadoopConfig = helper.loadHadoopConfig(hadoopPropsFile);
    }
    return hadoopConfig;
  }
  
  private org.apache.hadoop.conf.Configuration minHadoopConfig = null;
  org.apache.hadoop.conf.Configuration getMinimalHadoopConfig() throws ConfigurationException{
    if (minHadoopConfig == null) {
      minHadoopConfig = helper.loadMinimalHadoopConfig(hadoopPropsFile);
    }
    return minHadoopConfig;
  }

  private HdfsUtils hdfsUtils;
  public HdfsUtils getHdfsUtils() {
    if (hdfsUtils == null) {
      try {
        hdfsUtils = new HdfsUtils(getMinimalHadoopConfig());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return hdfsUtils;
  }

}
