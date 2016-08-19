package net.deelam.graphtools.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.api.hadoop.HdfsService;

/**
 * @author dnlam
 *
 */
@Slf4j
public class HdfsServiceImpl implements HdfsService {
  
  private final HdfsUtils hdfs;
  
  public HdfsServiceImpl(Configuration hadoopConfig) {
    hdfs=new HdfsUtils(hadoopConfig);
  }

  @Override
  public CompletableFuture<String> downloadFile(String src, String dst) throws IOException {
    Path srcPath = new Path(src);
    log.debug("Copying file: {} -> {}", srcPath, new Path(dst));
    try {
      String localFile=hdfs.downloadFile(src, dst);
      log.info("Copied file to: {}", localFile);
      return CompletableFuture.completedFuture(localFile);
    } catch (Throwable e){ // exceptions are not obvious on RPC client so print them here
      e.printStackTrace();
      throw e;
    }
  }

  @Override
  public CompletableFuture<String> uploadFile(String localFile, String destPath, boolean overwrite) throws IllegalArgumentException, IOException {
    try{
      hdfs.uploadFile(new File(localFile), new Path(destPath), overwrite);
      return CompletableFuture.completedFuture(destPath);
    } catch (Throwable e){ // exceptions are not obvious on RPC client so print them here
      e.printStackTrace();
      throw e;
    }
  }
  
  @Override
  public CompletableFuture<List<String>> listDir(String path, boolean recursive) throws IOException {
    try{
      List<String> files=hdfs.listDir(path,recursive);
      return CompletableFuture.completedFuture(files);
    } catch (Throwable e){ // exceptions are not obvious on RPC client so print them here
      e.printStackTrace();
      throw e;
    }
  }
  
  @Override
  public CompletableFuture<Boolean> exists(String path) throws IOException {
    try{
      boolean exists=hdfs.exists(path);
      return CompletableFuture.completedFuture(Boolean.valueOf(exists));
    } catch (Throwable e){ // exceptions are not obvious on RPC client so print them here
      e.printStackTrace();
      throw e;
    }
  }
}
