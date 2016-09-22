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
  
  private static final String HDFS_SVC_READY_FILE = "hdfsSvcReady.txt";
  static {
    File file = new File(HDFS_SVC_READY_FILE);
    if(file.exists() && !file.delete())
      log.warn("Could not delete file: {}", file);
  }

  private final HdfsUtils hdfs;
  
  public HdfsServiceImpl(Configuration hadoopConfig) {
    hdfs=new HdfsUtils(hadoopConfig);
    
    System.out.println("===================== All HDFS RPC service ready ======================");
    File file = new File(HDFS_SVC_READY_FILE);
    try {
      file.createNewFile();
      file.deleteOnExit();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public CompletableFuture<String> downloadFile(String src, String dst) throws IOException {
    log.info("RPC: downloadFile({}, {})", src, dst);
    try {
      String localFile=hdfs.downloadFile(src, dst);
      return CompletableFuture.completedFuture(localFile);
    } catch (Throwable e){ // exceptions are not obvious on RPC client so print them here
      e.printStackTrace();
      throw e;
    }
  }

  @Override
  public CompletableFuture<String> uploadFile(String localFile, String destPath, boolean overwrite) throws IllegalArgumentException, IOException {
    log.info("RPC: uploadFile({}, {}, {})", localFile, destPath, overwrite);
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
    log.info("RPC: listDir({}, {})", path, recursive);
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
    log.info("RPC: exists({})", path);
    try{
      boolean exists=hdfs.exists(path);
      return CompletableFuture.completedFuture(Boolean.valueOf(exists));
    } catch (Throwable e){ // exceptions are not obvious on RPC client so print them here
      e.printStackTrace();
      throw e;
    }
  }
  
  @Override
  public CompletableFuture<String> ensureDirExists(String path) throws IOException {
    log.info("RPC: ensureDirExists({})", path);
    try{
      Path qualPath=hdfs.ensureDirExists(path);
      return CompletableFuture.completedFuture(qualPath.toUri().toString());
    } catch (Throwable e){ // exceptions are not obvious on RPC client so print them here
      e.printStackTrace();
      throw e;
    }
  }
  
  @Override
  public CompletableFuture<Boolean> delete(String path) throws IOException {
    log.info("RPC: delete({})", path);
    try{
      boolean success=hdfs.delete(path);
      return CompletableFuture.completedFuture(Boolean.valueOf(success));
    } catch (Throwable e){ // exceptions are not obvious on RPC client so print them here
      e.printStackTrace();
      throw e;
    }
  }
}
