package net.deelam.vertx;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.hazelcast.config.Config;

import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import lombok.extern.slf4j.Slf4j;
/*
VertxOptions options = new VertxOptions(); //.setClustered(true);

Vertx vertx = VerticleUtils.initVertx(options, runner, jc, prod, cons);
try {
  log.info("Allowing time for completion...");
  Thread.sleep(3000);
} catch (InterruptedException e) {
  e.printStackTrace();
}
log.info("Closing Vertx");
vertx.close();
*/
@Slf4j
public final class VerticleUtils {

  public static Vertx initVertx(VertxOptions options, Consumer<Vertx> deployer) {
    if (options.isClustered()) {
      Config hazelcastConfig = new Config();
      // Now set some stuff on the config (omitted)
      ClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);
      options.setClusterManager(mgr);

      Vertx.clusteredVertx(options, res -> {
        if (res.succeeded()) {
          Vertx vertx = res.result();
          deployer.accept(vertx);
        } else {
          res.cause().printStackTrace();
        }
      });
      return null;
    } else {
      Vertx vertx = Vertx.vertx(options);
      deployer.accept(vertx);
      return vertx;
    }
  }

  public static Vertx initVertx(VertxOptions options, Verticle verticle,
      DeploymentOptions deploymentOptions, Handler<AsyncResult<String>> completionHandler) {
    Consumer<Vertx> deployer = vertx -> {
      try {
        if (deploymentOptions != null) {
          vertx.deployVerticle(verticle, deploymentOptions, completionHandler);
        } else {
          vertx.deployVerticle(verticle, completionHandler);
        }
      } catch (Throwable t) {
        t.printStackTrace();
        throw t;
      }
    };
    return initVertx(options, deployer);
  }

  public static Vertx initVertx(VertxOptions options, Consumer<Vertx> runner,  Verticle... verticles) {
    Consumer<Vertx> deployer = vertx -> {
      try {
        final AtomicInteger count=new AtomicInteger(verticles.length);
        for (Verticle v : verticles) {
          vertx.deployVerticle(v, res -> {
            if(count.decrementAndGet()==0)
              runner.accept(vertx);
          });
        }
      } catch (Throwable t) {
        t.printStackTrace();
        throw t;
      }
    };
    return initVertx(options, deployer);
  }
  
  ////
  
  /**
   * Verticles can be deployed in any order and find each other.
   * Given a client and server verticles,
   * - client deploys first and publishes "I'm here as client"; server deploys and publishes "I'm here as server and my address is ..."; client uses broadcasted address   
   * - client deploys second and publishes "I'm here as client with address"; server sends "My address is ..." to client; client uses response to register/use/etc
   * 
   * Client needs address handler.
   * Server needs address publisher and responder.
   * Both need I'm-here registration publisher.
   */

  private static final String YP_ADDRESS_PREFIX = "net.deelam.vertx.";

  public static MessageConsumer<Object> announceServiceType(Vertx vertx, String type, String serviceContactInfo){
    log.info("Announcing service type={}: {}", type, serviceContactInfo);
    // in response to client's broadcast, notify that particular client (Vertx does not allow msg.reply())
    MessageConsumer<Object> consumer = vertx.eventBus().consumer(YP_ADDRESS_PREFIX+"clients."+type, msg ->{
      String clientAddress=(String) msg.body();
      vertx.eventBus().send(clientAddress, serviceContactInfo);
    });
    
    vertx.eventBus().publish(YP_ADDRESS_PREFIX+"servers."+type, serviceContactInfo); // server's broadcast
    return consumer;
  }

  static int clientCount=0;
  static long startMillis=System.currentTimeMillis();
  
  /**
   * 
   * @param vertx
   * @param type
   * @param serverRespHandler may be called more than once for the same server
   * @return client's inbox event bus address
   */
  public synchronized static String announceClientType(Vertx vertx, String type, Handler<Message<String>> serverRespHandler){
    log.info("Announcing client type={}", type);
    
    vertx.eventBus().consumer(YP_ADDRESS_PREFIX+"servers."+type, (Message<String> msg) ->{
      log.info("Got server broadcast: {}", msg.body());
      serverRespHandler.handle(msg); 
    }); // handle server's broadcast
    
    String myAddress=YP_ADDRESS_PREFIX+type+".clientInbox_"+(++clientCount)+"_"+(System.currentTimeMillis()-startMillis);
    vertx.eventBus().consumer(myAddress, (Message<String> msg) ->{
      log.info("{}: Got server reply: {}", myAddress, msg.body());
      serverRespHandler.handle(msg); 
    }); // handle server response to client's broadcast
    
    vertx.eventBus().publish(YP_ADDRESS_PREFIX+"clients."+type, myAddress); // client's broadcast
    return myAddress;
  }

}

