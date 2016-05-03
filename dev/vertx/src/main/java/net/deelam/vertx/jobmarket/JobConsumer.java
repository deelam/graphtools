package net.deelam.vertx.jobmarket;

import java.util.concurrent.Future;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.jobmarket.JobMarket.BUS_ADDR;

/**
 * 
 * @author dd
 */
@Slf4j
@RequiredArgsConstructor
public abstract class JobConsumer extends AbstractVerticle {
  final String jcPrefix;
  DeliveryOptions consumerDeliveryOptions;

  JsonObject picked;
  
  @Override
  public void start() throws Exception {
    String myAddr=deploymentID();
    log.info("Ready: " + this + " myAddr="+myAddr);
    consumerDeliveryOptions = JobMarket.createConsumerHeader(myAddr);
    
    Handler<Message<Object>> handler = msg -> {
      JsonArray jobs=(JsonArray) msg.body();
      log.info("Jobs={}", jobs);
      if(jobs.size()>0){
        picked=jobs.getJsonObject(0);
        log.info("picked={}", picked);
        msg.reply(picked, consumerDeliveryOptions, reply ->{
          if(reply.failed())
            log.error("pickedError ", reply.cause());
          else
            log.info("picked sent"+reply.result().body());
        });
        if(doWork(picked))
          jobDone();
        else
          jobFailed();
      }
    };
    vertx.eventBus().consumer(myAddr, handler);
  }

  public abstract boolean doWork(JsonObject job);

  public void register(){
    vertx.eventBus().send(jcPrefix + BUS_ADDR.REGISTER, null, consumerDeliveryOptions, defaultReplyHandler);
  }

  public void jobDone(){
    vertx.eventBus().send(jcPrefix + BUS_ADDR.DONE, picked, consumerDeliveryOptions, defaultReplyHandler);
    picked=null;
  }
  
  public void jobFailed() {
    vertx.eventBus().send(jcPrefix + BUS_ADDR.FAIL, picked, consumerDeliveryOptions, defaultReplyHandler);
    picked=null;
  }

  private Handler<AsyncResult<Message<JsonObject>>> defaultReplyHandler = (reply) -> {
    if (reply.succeeded()) {
    } else if (reply.failed()) {
      log.error("Failed: " + reply.cause());
    } else {
      log.info("unknown reply: " + reply);
    }
  };


}
