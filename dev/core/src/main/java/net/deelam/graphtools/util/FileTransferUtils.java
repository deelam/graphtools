package net.deelam.graphtools.util;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.FileUtils;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.api.hadoop.HdfsService;

@Slf4j
public class FileTransferUtils {

  public static File retrieveFile(HdfsService hdfs, String srcFileUri, File destDir) 
      throws IOException, InterruptedException, ExecutionException {
    return retrieveFile(hdfs, srcFileUri, destDir, false);
  }
  
  public static File retrieveFile(HdfsService hdfs, String srcFileUri, File destDir, boolean deleteSrcHdfsFile) 
      throws IOException, InterruptedException, ExecutionException {
    URI originUri = URI.create(srcFileUri);
    File currFile=null;
    switch(originUri.getScheme()){
      case "file":
        currFile = new File(originUri);
        break;
      case "hdfs":{
        File origFile = new File(srcFileUri);
        CompletableFuture<String> localFile = hdfs.downloadFile(originUri.toString(), 
            destDir.getAbsolutePath()+"/"+System.currentTimeMillis()+"-"+origFile.getName());
        currFile=new File(localFile.get());
        log.info("currFile={}", currFile);
        if(deleteSrcHdfsFile){
          log.info("Deleting sourceFile={}", originUri);
          hdfs.delete(originUri.toString());
        }
        break;
      }
      default: throw new IllegalArgumentException("Unknown scheme for: "+originUri);
    }
    return currFile;
  }

  public static void main(String[] args) throws URISyntaxException, IOException {
    String destPath="foo/bar";
    URI destUri = new URI(destPath);
    String scheme=destUri.getScheme();
    if(scheme==null){
      scheme="file";
      destUri=new File(destPath).toURI();
    }
    System.out.println(destUri);
    
    FileUtils.moveDirectory(new File("ok"), new File("good"));
  }
  
  public static void sendFile(HdfsService hdfs, File localFile, String destPath, boolean deleteLocalFile) 
      throws IOException {
    checkArgument(localFile.exists(), "Source file does not exist: "+localFile);
    URI destUri = URI.create(destPath);
    String scheme=destUri.getScheme();
    if(scheme==null){
      scheme="file";
      destUri=new File(destPath).toURI();
    }
    switch (scheme) {
      case "file":{
        File destFile = new File(destUri);
        if(destFile.exists())
          throw new FileAlreadyExistsException("Cannot copy "+localFile.getAbsolutePath()+" to "+destFile.getAbsolutePath());
        if(!localFile.equals(destFile)){
          if(deleteLocalFile){
            log.info("Moving {} to {}", localFile, destFile);
            if(localFile.isDirectory())
              FileUtils.moveDirectory(localFile, destFile);
            else
              FileUtils.moveFile(localFile, destFile);
          } else {
            log.info("Copying {} to {}", localFile, destFile);
            if(localFile.isDirectory())
              FileUtils.copyDirectory(localFile, destFile);
            else
              FileUtils.copyFile(localFile, destFile);
          }
        }
        break;
      }
      case "hdfs": {
        try {
          CompletableFuture<Boolean> destExists = hdfs.exists(destPath);
          if(destExists.get().booleanValue())
            throw new FileAlreadyExistsException("Cannot copy "+localFile.getAbsolutePath()+" to "+destPath);
        } catch (InterruptedException | ExecutionException e) {
          throw new IOException(e);
        }
        
        CompletableFuture<String> uploaded = hdfs.uploadFile(localFile.getAbsolutePath(), destPath, false);
        try {
          log.info("Uploaded to {}", uploaded.get());
        } catch (InterruptedException | ExecutionException e) {
          throw new IOException(e);
        }
        if(deleteLocalFile){
          log.debug("Deleting local file: {}", localFile);
          FileUtils.forceDelete(localFile);
        }
        break;
        }
      default:
        throw new IllegalArgumentException("Unknown scheme for: " + destUri);
    }
  }

  public static void moveFile(HdfsService hdfs, String localPath, String destPath) throws IOException {
    File file = new File(localPath);
    try{
      sendFile(hdfs, file, destPath, true);
    }catch(Exception e){
      log.error("Problem uploading to hdfs; local file is available at {}", file.getAbsolutePath());
      throw e;
    }
  }

  
}

