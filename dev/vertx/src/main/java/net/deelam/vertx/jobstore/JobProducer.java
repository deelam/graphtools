package net.deelam.vertx.jobstore;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @author dd
 */
@Slf4j
public class JobProducer extends AbstractVerticle {
  
  private static final String JOBCOMPLETE_ADDRESS_SUFFIX = "-jobComplete";
  
//  @Setter
//  private Handler<Message<Object>> jobCompletionHandler;
  
  private String addJobBusAddr;
  private String jobCompletionAddress;

  public JobProducer(String addJobBusAddress) {
    this.addJobBusAddr = addJobBusAddress;
  }

  @Override
  public void start() throws Exception {
    jobCompletionAddress=deploymentID()+JOBCOMPLETE_ADDRESS_SUFFIX;
    
//    if(jobCompletionHandler!=null){
//      log.info("set jobCompletionHandler={}", jobCompletionHandler);
//      vertx.eventBus().consumer(jobCompletionAddress, jobCompletionHandler);
//    }
    
    log.info("Ready: " + this +" jobCompletionAddress="+jobCompletionAddress);
  }

  public void addJobCompletionHandler(Handler<Message<Object>> jobCompletionHandler) {
    log.info("add jobCompletionHandler={}", jobCompletionHandler);
    vertx.eventBus().consumer(jobCompletionAddress, jobCompletionHandler);
  }
  
  public void addJob(JobJO job) {
    addJob(job, defaultAddJobReplyHandler);
  }

  public void addJob(JobJO job, Handler<AsyncResult<Message<JsonObject>>> replyH) {
    job.setJobCompletionAddress(jobCompletionAddress);
    vertx.eventBus().send(addJobBusAddr, job, replyH);
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
