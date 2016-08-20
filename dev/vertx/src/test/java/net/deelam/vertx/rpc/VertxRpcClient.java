package net.deelam.vertx.rpc;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.StartVertx;
import net.deelam.vertx.inject.VertxRpcHelper;

@Slf4j
public class VertxRpcClient {

  static Vertx vertx; // = Vertx.vertx();
  
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    
    VertxOptions options=new VertxOptions().setClustered(true);
    StartVertx.create(options, false, null, 12345, vertX -> {
      log.info("Vert.x service registered");
      vertx=vertX;
    });

    try {
      Thread.sleep(3000);
    } catch (Exception e) {
      e.printStackTrace();
    }

    HdfsInterface hdfs = new VertxRpcHelper<HdfsInterface>(vertx, "Address")
        .clientOptions(c->c.setTimeout(6_000L))
        .getClient(HdfsInterface.class);
    
    try {
      go(hdfs);
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    
    System.out.println("Done");
    try {
      Thread.sleep(3000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    vertx.close();
    System.out.println("Closed");

//    HdfsSvc hdfsSvc = new HdfsSvc();
//    RPCServerOptions serverOption =
//        new RPCServerOptions(vertx).setBusAddress("Address").addService(hdfsSvc);
//    RPCServer rpcServer = new VertxRPCServer(serverOption);


//    RPCClientOptions<HdfsInterface> rpcClientOptions = new RPCClientOptions<HdfsInterface>(vertx).setBusAddress("Address")
//        .setServiceClass(HdfsInterface.class);
//    HdfsInterface hdfs = new VertxRPCClient<>(rpcClientOptions).bindService();

  }

  private static void go(HdfsInterface hdfs) throws Exception{
    if(true){
      try {
        CompletableFuture<Boolean> exists = hdfs.exists("src");
        System.out.println("exists="+exists.get());
        CompletableFuture<String> up = hdfs.uploadFile("localFile", "destPath", false);
        System.out.println("up="+up.get());
        
        CompletableFuture<File> ffile = hdfs.downloadFile("src", "dst");
        System.out.println("file="+ffile.get());
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      //invoking service async
      try {
        Handler<AsyncResult<HdfsInterface>> handler = asyncResult -> {
          if (asyncResult.succeeded()) {
            System.out.println("result: "+asyncResult.result().getClass()+" "+asyncResult.result());
            //System.out.println((hdfsSvc==asyncResult.result())+" "+hdfsSvc);
          } else {
            System.err.println("result: "+asyncResult.cause());
            asyncResult.cause().printStackTrace();
          }
        };
        System.out.println("handler="+handler.getClass());
        hdfs.downloadFile("src", "dst", handler);
        
        hdfs.downloadFile(null, "dst", asyncResult -> {
          if (asyncResult.succeeded()) {
            System.out.println("result: "+asyncResult.result().getClass()+" "+asyncResult.result());
          } else {
            System.err.println("result: "+asyncResult.cause());
            asyncResult.cause().printStackTrace();
          }
        });
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
