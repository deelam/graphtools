package net.deelam.vertx.hadoop;

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

public class VertxRpc {

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Vertx vertx = Vertx.vertx();
    HdfsSvc hdfsSvc = new HdfsSvc();
    RPCServerOptions serverOption =
        new RPCServerOptions(vertx).setBusAddress("Address").addService(hdfsSvc);
    RPCServer rpcServer = new VertxRPCServer(serverOption);


    RPCClientOptions<Hdfs> rpcClientOptions = new RPCClientOptions<Hdfs>(vertx).setBusAddress("Address")
        .setServiceClass(Hdfs.class);
    Hdfs hdfs = new VertxRPCClient<>(rpcClientOptions).bindService();

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
        Handler<AsyncResult<Hdfs>> handler = asyncResult -> {
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
