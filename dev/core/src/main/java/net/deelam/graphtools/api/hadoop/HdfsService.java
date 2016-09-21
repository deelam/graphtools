package net.deelam.graphtools.api.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;


/**
 * @author dnlam
 *
 */
public interface HdfsService {

  /// Reminder: Returned object within CompletableFuture must be marshallable by vert-rpc (e.g., File is not)
  
  CompletableFuture<String> downloadFile(String src, String dst) throws IOException;

  CompletableFuture<String> uploadFile(String localFile, String destPath, boolean overwrite) throws IllegalArgumentException, IOException;

  CompletableFuture<List<String>> listDir(String path, boolean recursive) throws IOException;

  CompletableFuture<Boolean> exists(String path) throws IOException;

  CompletableFuture<String> ensureDirExists(String path) throws IOException;

}
