package net.deelam.vertx.jobstore;

import static net.deelam.vertx.jobstore.JobConsigner.JOB_COMPLETED_ATTRIBUTE;
import static net.deelam.vertx.jobstore.JobConsigner.JOB_FAILED_ATTRIBUTE;
import static net.deelam.vertx.jobstore.JobConsigner.JOB_STARTED_ATTRIBUTE;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @author dd
 */
@Slf4j
@RequiredArgsConstructor
public class JobConsumer extends AbstractVerticle {

  private final String listJobsBusAddr, updateJobBusAddr;

  @Override
  public void start() throws Exception {
    log.info("Ready: " + this);
  }

  public void listJobs(Handler<AsyncResult<Message<JsonArray>>> replyH) {
    vertx.eventBus().send(listJobsBusAddr, null, replyH);
  }

  public void updateJob(JobJO job) {
    job.setUpdateType("");
    sendUpdateJob(job);
  }

  public void startedJob(JobJO job) {
    job.setUpdateType(JOB_STARTED_ATTRIBUTE);
    log.info("Started job: {}", job);
    sendUpdateJob(job);
  }

  public void completedJob(JobJO job) {
    job.setUpdateType(JOB_COMPLETED_ATTRIBUTE);
    sendUpdateJob(job);
  }

  public void failedJob(JobJO job) {
    job.setUpdateType(JOB_FAILED_ATTRIBUTE);
    sendUpdateJob(job);
  }

  private void sendUpdateJob(JobJO job) {
    vertx.eventBus().send(updateJobBusAddr, job, defaultUpdateJobReplyHandler);
  }

  private Handler<AsyncResult<Message<JsonObject>>> defaultUpdateJobReplyHandler = (reply) -> {
    if (reply.succeeded()) {
    } else if (reply.failed()) {
      log.error("updateJob Failed: " + reply.cause());
    } else {
      log.info("updateJob unknown reply: " + reply);
    }
  };


}
