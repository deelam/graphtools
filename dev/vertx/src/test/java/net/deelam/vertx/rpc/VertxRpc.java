package net.deelam.vertx.rpc;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import as.leap.vertx.rpc.RPCServer;
import as.leap.vertx.rpc.impl.RPCClientOptions;
import as.leap.vertx.rpc.impl.RPCServerOptions;
import as.leap.vertx.rpc.impl.VertxRPCClient;
import as.leap.vertx.rpc.impl.VertxRPCServer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import net.deelam.graphtools.api.hadoop.HdfsService;
import net.deelam.vertx.inject.VertxRpcHelper;

public class VertxRpc {

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Vertx vertx = Vertx.vertx();
    HdfsSvc hdfsSvc = new HdfsSvc();
    RPCServerOptions serverOption =
        new RPCServerOptions(vertx).setBusAddress("Address").addService(hdfsSvc);
    RPCServer rpcServer = new VertxRPCServer(serverOption);


//    RPCClientOptions<HdfsInterface> rpcClientOptions = new RPCClientOptions<HdfsInterface>(vertx).setBusAddress("Address")
//        .setServiceClass(HdfsInterface.class);
//    HdfsInterface hdfs = new VertxRPCClient<>(rpcClientOptions).bindService();

    HdfsInterface hdfs = new VertxRpcHelper<HdfsInterface>(vertx, "Address")
    .clientOptions(c->c.setTimeout(10_000L))
    .getClient(HdfsInterface.class);
    
    if(true){
      try {
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
            System.out.println((hdfsSvc==asyncResult.result())+" "+hdfsSvc);
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
    
    
    System.out.println("Done");
    Thread.sleep(3000);
    vertx.close();
    System.out.println("Closed");
  }
}
