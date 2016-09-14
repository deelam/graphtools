package net.deelam.vertx.jobmarket2;

import static com.google.common.base.Preconditions.checkNotNull;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.VerticleUtils;
import net.deelam.vertx.jobmarket2.JobMarket.BUS_ADDR;

/**
 * 
 * @author dd
 */
@RequiredArgsConstructor
@Slf4j
@ToString
public class JobProducer extends AbstractVerticle {
  private final String serviceType;
  
  private static final String JOBCOMPLETE_ADDRESS_SUFFIX = "-jobComplete";
  private static final String JOBFAILED_ADDRESS_SUFFIX = "-jobFailed";
  
  private String jmPrefix=null;
  private String jobCompletionAddress=null;
  private String jobFailureAddress=null;

  @Override
  public void start() throws Exception {
    log.info("Ready: deploymentID={} this={}", deploymentID(), this);
    
    VerticleUtils.announceClientType(vertx, serviceType, msg->{
      jmPrefix=msg.body();
      checkNotNull(jmPrefix);
    });
  }
  
  public boolean isReady(){
    return jmPrefix!=null;
  }

  public <T> void addJobCompletionHandler(Handler<Message<JobDTO>> jobCompletionHandler) {
    jobCompletionAddress=deploymentID()+JOBCOMPLETE_ADDRESS_SUFFIX;
    log.info("add jobCompletionHandler to address={}", jobCompletionAddress);
    vertx.eventBus().consumer(jobCompletionAddress, jobCompletionHandler);
  }

  public <T> void addJobFailureHandler(Handler<Message<JobDTO>> jobFailureHandler) {
    jobFailureAddress=deploymentID()+JOBFAILED_ADDRESS_SUFFIX;
    log.info("add jobFailureHandler to address={}", jobFailureAddress);
    vertx.eventBus().consumer(jobFailureAddress, jobFailureHandler);
  }

  public void addJob(JobDTO job) {
    waitUntilReady();
    DeliveryOptions deliveryOpts = JobMarket.createProducerHeader(jobCompletionAddress, jobFailureAddress, 0);
    vertx.eventBus().send(jmPrefix + BUS_ADDR.ADD_JOB, job, deliveryOpts, addJobReplyHandler);
  }

  private void waitUntilReady() {
    while (!isReady())
      try {
        log.info("Waiting for jobProducer to find jobMarket");
        Thread.sleep(1000); // wait until connected to jobMarket
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting",e);
      }
  }

  public void removeJob(String jobId, Handler<AsyncResult<Message<JobDTO>>> removeJobReplyHandler) {
    vertx.eventBus().send(jmPrefix + BUS_ADDR.REMOVE_JOB, jobId, 
        (removeJobReplyHandler == null) ? this.removeJobReplyHandler : removeJobReplyHandler);
  }

  public void getProgress(String jobId, Handler<AsyncResult<Message<JobDTO>>> handler) {
    vertx.eventBus().send(jmPrefix + BUS_ADDR.GET_PROGRESS, jobId, 
        (handler == null) ? this.progressReplyHandler : handler);
  }

  @Setter
  private Handler<AsyncResult<Message<JobDTO>>> addJobReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.debug("Job added: {}", reply.result().body());
    } else if (reply.failed()) {
      log.error("addJob failed: ", reply.cause());
    } else {
      log.warn("addJob unknown reply: {}", reply);
    }
  };
  
  @Setter
  private Handler<AsyncResult<Message<JobDTO>>> removeJobReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.debug("Job removed: {}", reply.result().body());
    } else if (reply.failed()) {
      log.error("removeJob failed: ", reply.cause());
    } else {
      log.warn("removeJob unknown reply: {}", reply);
    }
  };
  
  @Setter
  private Handler<AsyncResult<Message<JobDTO>>> progressReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.debug("Job progress: {}", reply.result().body());
    } else if (reply.failed()) {
      log.error("jobProgress failed: ", reply.cause());
    } else {
      log.warn("jobProgress unknown reply: {}", reply);
    }
  };
}
