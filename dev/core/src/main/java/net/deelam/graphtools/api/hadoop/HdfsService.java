package net.deelam.graphtools.api.hadoop;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * @author dnlam
 *
 */
public interface HdfsService {

  /// Reminder: Returned object within CompletableFuture must be serializable by vert-rpc (e.g., File is not)
  
  CompletableFuture<String> downloadFile(String src, String dst) throws IOException;

}
