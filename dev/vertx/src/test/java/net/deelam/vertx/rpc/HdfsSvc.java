package net.deelam.vertx.rpc;



import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HdfsSvc implements HdfsInterface {
  
  public void downloadFile(String src, String dst, Handler<AsyncResult<HdfsInterface>> handler) throws IOException {
    try {
      log.info("Working ..");
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    log.info("... done working");
    if (src==null)
      throw new IOException("test Exception");
    
    System.out.println("Calling handler "+handler.getClass());
    handler.handle(Future.succeededFuture(this));
  }
  
  
  @Override
  public CompletableFuture<File> downloadFile(String src, String dst) throws IOException {
    try {
      log.info("Working ..");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
      log.info("... done working");
   
    if (!true)
      throw new IOException("test Exception");
    
    return CompletableFuture.completedFuture(new File(src));
  }

}
