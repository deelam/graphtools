package net.deelam.vertx.jobmarket;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.jobmarket.JobMarket.BUS_ADDR;

/**
 * 
 * @author dd
 */
@RequiredArgsConstructor
@Slf4j
public class JobProducer extends AbstractVerticle {
  
  private static final String JOBCOMPLETE_ADDRESS_SUFFIX = "-jobComplete";
  
  private final String jcPrefix;
  private String jobCompletionAddress;

  @Override
  public void start() throws Exception {
    jobCompletionAddress=deploymentID()+JOBCOMPLETE_ADDRESS_SUFFIX;
    log.info("Ready: " + this +" jobCompletionAddress="+jobCompletionAddress);
  }

  public void addJobCompletionHandler(Handler<Message<Object>> jobCompletionHandler) {
    log.info("add jobCompletionHandler={}", jobCompletionHandler);
    vertx.eventBus().consumer(jobCompletionAddress, jobCompletionHandler);
  }
  
  public void addJob(String jobId, JsonObject job) {
    DeliveryOptions producerDeliveryOpts = JobMarket.createProducerHeader(jobId, jobCompletionAddress);
    vertx.eventBus().send(jcPrefix + BUS_ADDR.ADD_JOB, job, producerDeliveryOpts, defaultAddJobReplyHandler);
  }

  private Handler<AsyncResult<Message<JsonObject>>> defaultAddJobReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.info("addJob reply={}", reply.result().body());
    } else if (reply.failed()) {
      log.error("addJob Failed: " + reply.cause());
    } else {
      log.info("addJob unknown reply: " + reply);
    }
  };

}
