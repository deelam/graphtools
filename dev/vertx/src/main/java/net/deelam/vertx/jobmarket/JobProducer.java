package net.deelam.vertx.jobmarket;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
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
  
  private final String jmPrefix;
  private String jobCompletionAddress=null;

  @Override
  public void start() throws Exception {
    log.info("Ready: " + this +" deploymentID="+deploymentID());
  }

  public <T> void addJobCompletionHandler(Handler<Message<T>> jobCompletionHandler) {
    jobCompletionAddress=deploymentID()+JOBCOMPLETE_ADDRESS_SUFFIX;
    log.info("add jobCompletionHandler={}", jobCompletionHandler);
    vertx.eventBus().consumer(jobCompletionAddress, jobCompletionHandler);
  }
  
  public void addJob(String jobId, JsonObject job) {
    DeliveryOptions deliveryOpts = JobMarket.createProducerHeader(jobId, jobCompletionAddress);
    vertx.eventBus().send(jmPrefix + BUS_ADDR.ADD_JOB, job, deliveryOpts, addJobReplyHandler);
  }

  public void removeJob(String jobId) {
    DeliveryOptions deliveryOpts = JobMarket.createProducerHeader(jobId, null);
    vertx.eventBus().send(jmPrefix + BUS_ADDR.REMOVE_JOB, null, deliveryOpts, removeJobReplyHandler);
  }

  public <T> void getProgress(String jobId, Handler<AsyncResult<Message<T>>> handler) {
    DeliveryOptions deliveryOpts = JobMarket.createProducerHeader(jobId, null);
    vertx.eventBus().send(jmPrefix + BUS_ADDR.GET_PROGRESS, null, deliveryOpts, handler);
  }

  @Setter
  private Handler<AsyncResult<Message<JsonObject>>> addJobReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.debug("Job added: {}", reply.result().body());
    } else if (reply.failed()) {
      log.error("addJob failed: ", reply.cause());
    } else {
      log.warn("addJob unknown reply: {}", reply);
    }
  };
  
  @Setter
  private Handler<AsyncResult<Message<JsonObject>>> removeJobReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.debug("Job removed: {}", reply.result().body());
    } else if (reply.failed()) {
      log.error("removeJob failed: ", reply.cause());
    } else {
      log.warn("removeJob unknown reply: {}", reply);
    }
  };
}
