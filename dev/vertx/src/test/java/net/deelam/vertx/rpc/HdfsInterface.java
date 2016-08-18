/**
 * 
 */
package net.deelam.vertx.rpc;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * @author dnlam
 *
 */
public interface HdfsInterface {
  CompletableFuture<File> downloadFile(String src, String dst) throws IOException;

  void downloadFile(String src, String dst, Handler<AsyncResult<HdfsInterface>> handler) throws IOException;
}
