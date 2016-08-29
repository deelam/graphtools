package net.deelam.vertx.inject;

import java.util.function.Consumer;

import as.leap.vertx.rpc.impl.RPCClientOptions;
import as.leap.vertx.rpc.impl.RPCServerOptions;
import as.leap.vertx.rpc.impl.VertxRPCClient;
import as.leap.vertx.rpc.impl.VertxRPCServer;
import io.protostuff.runtime.RuntimeEnv;
import io.vertx.core.Vertx;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class VertxRpcHelper<T> {

  static{
   log.info("RuntimeEnv: USE_SUN_MISC_UNSAFE="+RuntimeEnv.USE_SUN_MISC_UNSAFE);
	if(RuntimeEnv.USE_SUN_MISC_UNSAFE==false){
		throw new RuntimeException("Make sure sun.reflect is available in OSGi so that RuntimeEnv can access Vertx-RPC's implicit no-args WrapperType constructor");
	}
  }


  private final Vertx vertx;
  private final String busAddr;

  ///
  
  public static <T> void registerService(Vertx vertx, String busAddr, T service) {
    VertxRpcHelper<T> rpc = new VertxRpcHelper<T>(vertx, busAddr);
    rpc.registerService(service);
  }

  public static <T> T getClient(Vertx vertx, String busAddr, Class<T> serviceClass) {
    VertxRpcHelper<T> rpc = new VertxRpcHelper<T>(vertx, busAddr);
    return rpc.getClient(serviceClass);
  }

  ///

  private RPCServerOptions serverOptions = null;

  public RPCServerOptions getServerOptions() {
    if (serverOptions == null) {
      serverOptions = new RPCServerOptions(vertx)
          .setBusAddress(busAddr);
    }
    return serverOptions;
  }

  public void registerService(T service) {
    new VertxRPCServer(getServerOptions().addService(service));
    log.info("Created RPC service: {}", service.getClass());
  }

  ///

  private RPCClientOptions<T> clientOptions = null;
  public RPCClientOptions<T> getClientOptions() {
    if (clientOptions == null) {
      clientOptions = new RPCClientOptions<T>(vertx)
          .setBusAddress(busAddr);
    }
    return clientOptions;
  }

  public VertxRpcHelper<T> clientOptions(Consumer<RPCClientOptions<T>> c) {
    c.accept(getClientOptions());
    return this;
  }

  public T getClient(Class<T> serviceClass) {
    T hdfs = new VertxRPCClient<>(getClientOptions().setServiceClass(serviceClass)).bindService();
    log.info("Created RPC client: {} for {}", hdfs.getClass(), serviceClass);
    return hdfs;
  }

}
