package net.deelam.vertx;

import static com.google.common.base.Preconditions.checkNotNull;

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

  private static Vertx initVertx(VertxOptions options, Consumer<Vertx> deployer) {
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
   * - If client deploys first and publishes "I'm here as client"; server deploys and publishes "I'm here as server and my address is ..."; client uses broadcasted address   
   * - If client deploys second and publishes "I'm here as client with address"; server sends "My address is ..." to client; client uses response to register/use/etc
   * 
   * Client needs serviceContactInfo handler for both server's broadcast and reply.
   * Server needs client responder.
   * Both need announcement publisher.
   */

  private static final String YP_ADDRESS_PREFIX = "net.deelam.";

  /**
   * Reminder: call this method after setting up the verticle's consumers to handle incoming messages.
   * @param vertx
   * @param type
   * @param serviceContactInfo
   * @return
   */
  public static MessageConsumer<Object> announceServiceType(Vertx vertx, String type, String serviceContactInfo){
    // TODO: add utility to detect existence of services listening on the same eventBus topic 
    // in response to client's broadcast, notify that particular client (Vertx does not allow msg.reply())
    String listenAddr = genServerListenAddr(type);
    log.info("Announcing service type={}: {} listening at {}", type, serviceContactInfo, listenAddr);
    MessageConsumer<Object> consumer = vertx.eventBus().consumer(listenAddr, msg ->{
      String clientAddress=(String) msg.body();
      log.debug("Got {} client registration from {}", type, clientAddress);
      connectWithClient(vertx, serviceContactInfo, clientAddress);
    });
    
    String publishAddr = genClientListenAddr(type);
    log.debug("Publishing to {}", publishAddr);
    vertx.eventBus().publish(publishAddr, serviceContactInfo); // server's broadcast
    return consumer;
  }

  private static String genClientListenAddr(String type) {
    return YP_ADDRESS_PREFIX+"servers."+type;
  }

  private static String genServerListenAddr(String type) {
    return YP_ADDRESS_PREFIX+"clients."+type;
  }

  private static void connectWithClient(Vertx vertx, String serviceContactInfo, String clientAddress) {
    vertx.eventBus().send(clientAddress, serviceContactInfo, clientResp -> {
      if (clientResp.failed()) {
        log.info("Didn't get ACK from client={}; retrying", clientAddress /*, clientResp.cause()*/);
        // to address Vertx problem of not sending response to ALL clients
        connectWithClient(vertx, serviceContactInfo, clientAddress); //retry
      }
    });    
  }

  static int clientCount=0;
  
  /**
   * 
   * @param vertx
   * @param serviceType
   * @param serverRespHandler may be called more than once for the same server
   * @return client's inbox event bus address
   */
  public synchronized static String announceClientType(Vertx vertx, String serviceType, Handler<Message<String>> serverRespHandler){
    String clientListenAddr = genClientListenAddr(serviceType);
    log.info("Announcing client of serviceType={}: listening at {} with handler={}", serviceType, clientListenAddr, serverRespHandler);
    vertx.eventBus().consumer(clientListenAddr, (Message<String> msg) ->{
      log.debug("Got server broadcast: serverAddr={}", msg.body());
      serverRespHandler.handle(msg); 
    }); // handle server's broadcast
    
    String myAddress=YP_ADDRESS_PREFIX+serviceType+".clientInbox_"+(++clientCount)+"_"+System.currentTimeMillis();
    vertx.eventBus().consumer(myAddress, (Message<String> msg) ->{
      log.debug("{}: Got server reply: {}", myAddress, msg.body());
      serverRespHandler.handle(msg); 
      msg.reply("Got it");
    }); // handle server response to client's broadcast
    
    String serverListenAddr = genServerListenAddr(serviceType);
    log.debug("Publishing client address={} for serviceType={} to {}", myAddress, serviceType, serverListenAddr);
    vertx.eventBus().publish(serverListenAddr, myAddress); // client's broadcast
    return myAddress;
  }

  ////
  
  public static String getConfig(AbstractVerticle v, String currVal, String configKey, String defaultVal) {
    final String configVal = v.config().getString(configKey);
    if (currVal == null) {
      currVal = configVal;
      if (currVal == null) {
        currVal = defaultVal;
        log.info("Using defaultValue={} for config={}", defaultVal, configKey);
      }
    } else if (configVal != null) {
      log.warn("Ignoring {} configuration since already set: {}", configKey, currVal);
    }

    checkNotNull(currVal, "Must set '" + configKey + "' config since not provided in constructor!");
    return currVal;
  }
}

