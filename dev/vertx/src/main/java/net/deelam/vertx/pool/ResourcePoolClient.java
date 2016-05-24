package net.deelam.vertx.pool;

import java.util.function.Consumer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.VerticleUtils;
import net.deelam.vertx.pool.PondVerticle.ADDR;


@RequiredArgsConstructor
@Slf4j
public class ResourcePoolClient extends AbstractVerticle {
  
  final String pondId;
  String pondAddr;
  
  @Setter
  Consumer<String> resourceConsumer;
  
  @Override
  public void start() throws Exception {
    VerticleUtils.announceClientType(vertx, pondId, msg->{
      pondAddr=msg.body();
      log.info(pondId+": found pond={}", pondAddr);
    });
    
    vertx.eventBus().consumer(deploymentID(), (Message<String> msg)->{
      log.info(pondId+": Got it! {}", msg.body());
      resourceConsumer.accept(msg.body());
    });
  }

  public void add(String resourceUri){
    vertx.eventBus().send(pondAddr+ADDR.ADD, resourceUri);
  }

  public void checkout(String resourceUri){
    JsonObject requestMsg = new JsonObject()
        .put(PondVerticle.CLIENT_ADDR, deploymentID())
        .put(PondVerticle.RESOURCE_URI, resourceUri);

    vertx.eventBus().send(pondAddr+ADDR.CHECKOUT, requestMsg);
  }
  
  public void checkin(String resourceUri){
    JsonObject requestMsg = new JsonObject()
        .put(PondVerticle.CLIENT_ADDR, deploymentID())
        .put(PondVerticle.RESOURCE_URI, resourceUri);

    vertx.eventBus().send(pondAddr+ADDR.CHECKIN, requestMsg);
  }

}