package net.deelam.vertx.rpc;

import io.vertx.core.VertxOptions;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.StartVertx;
import net.deelam.vertx.inject.VertxRpcHelper;

@Slf4j
public class VertxRpcServer {
  
  public static void main(String[] args) {
    VertxOptions options=new VertxOptions().setClustered(true);
    StartVertx.create(options, true, null, 12345, vertx -> {
      log.info("Vert.x service registered");
      
      HdfsSvc hdfsSvc = new HdfsSvc();
      VertxRpcHelper.registerService(vertx, "Address", hdfsSvc);
    });

  }

}
