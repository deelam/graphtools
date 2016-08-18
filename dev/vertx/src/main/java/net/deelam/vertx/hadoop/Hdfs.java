/**
 * 
 */
package net.deelam.vertx.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * @author dnlam
 *
 */
public interface Hdfs {
  CompletableFuture<File> downloadFile(String src, String dst) throws IOException;

  void downloadFile(String src, String dst, Handler<AsyncResult<Hdfs>> handler) throws IOException;
}
