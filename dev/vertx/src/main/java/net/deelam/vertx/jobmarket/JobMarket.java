package net.deelam.vertx.jobmarket;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.jobmarket.JobMarket.JobItem.JobState;

/**
 * JobProducer calls:
 * -addJob(id,completionAddr,job)
 * -getProgress(id)
 * -removeJob(id)
 * 
 * JobWorker calls:
 * -register(workerAddr)
 * -setProgress(job)
 * -done(job)
 * -fail(job)
 * 
 * JobWorkers register and JobProducers addJobs.  For each of these events, a list of available jobs are sent to an idle Worker to choose a job.
 * (If no job is chosen and the availableJobList has changed, the latest list is sent to the Worker.)
 * When a Worker is done or has failed the job, it is sent the latest availableJobList.
 * 
 * Workers can submit setProgress to update the job JsonObject.  100% progress can be set by sending a final setProgress(job) or done(job).
 * A job is simply marked as DONE.  To delete the job entry, call removeJob(id). 
 * 
 * If job fails, a failCount is incremented and the JOB_FAILEDCOUNT_ATTRIBUTE property set on the job JsonObject.
 * 
 * @author dd
 */
@Slf4j
@NoArgsConstructor
public class JobMarket extends AbstractVerticle {

  @Getter
  private String addressPrefix;

  public JobMarket(String addressPrefix) {
    this.addressPrefix = addressPrefix;
  }

  public static final String EVENT_BUS_PREFIX = "eventBusPrefix";

  private void initAddressPrefix() {
    final String configAddrPrefix = config().getString(EVENT_BUS_PREFIX);
    if (addressPrefix == null) {
      addressPrefix = configAddrPrefix;
      if (addressPrefix == null) {
        addressPrefix = deploymentID();
        log.info("Using deploymentID() as addressPrefix={}", addressPrefix);
      }
    } else if (configAddrPrefix != null) {
      log.warn("Ignoring {} configuration since already set (in constructor): {}", EVENT_BUS_PREFIX, addressPrefix);
    }

    checkNotNull(addressPrefix, "Must set '" + EVENT_BUS_PREFIX + "' config since not provided in constructor!");
  }

  public enum BUS_ADDR {
    ADD_JOB, REMOVE_JOB, GET_PROGRESS, // for producers 
    REGISTER, SET_PROGRESS, DONE, FAIL // for workers
  };

  private static final Object OK_REPLY = "ACK";

  // jobId -> JobItem
  private Map<String, JobItem> jobItems = new LinkedHashMap<>();

  private Queue<String> idleWorkers = new LinkedList<>();

  private Handler<AsyncResult<Message<JsonObject>>> debugReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.debug("reply={}", reply);
    } else if (reply.failed()) {
      log.error("failed: ", reply.cause());
    } else {
      log.warn("unknown reply: {}", reply);
    }
  };
  
  @Override
  public void start() throws Exception {
    initAddressPrefix();

    EventBus eb = vertx.eventBus();
    eb.consumer(addressPrefix + BUS_ADDR.REGISTER, message -> {
      String workerAddr = getWorkerAddress(message);
      log.debug("Received REGISTER message from {}", workerAddr);
      idleWorkers.add(workerAddr);
      sendJobsTo(workerAddr);
    });

    eb.consumer(addressPrefix + BUS_ADDR.ADD_JOB, message -> {
      log.debug("Received ADD_JOB message: {}", message.body());
      JobItem ji = new JobItem(message);
      ji.state = JobState.AVAILABLE;
      if (jobItems.containsKey(ji.jobId)) {
        message.fail(-11, "Job with id=" + ji.jobId + " already exists!");
      } else {
        message.reply(OK_REPLY);
        jobItems.put(ji.jobId, ji);
        sendJobsToNextIdleWorker();
      }
    });
    eb.consumer(addressPrefix + BUS_ADDR.REMOVE_JOB, message -> {
      String jobId = readJobId(message);
      log.debug("Received REMOVE_JOB message: jobId={}", jobId);
      if(jobItems.containsKey(jobId)){
        jobItems.remove(jobId);
        message.reply(OK_REPLY);
      } else {
        message.fail(-12, "Cannot find job with id=" + jobId);
      }
    });

    eb.consumer(addressPrefix + BUS_ADDR.SET_PROGRESS, message -> {
      log.debug("Received SET_PROGRESS message: {}", message.body());
      JsonObject jobJO = (JsonObject) message.body();
      JobItem job = getJobItem(jobJO);
      job.mergeIn(jobJO);
      job.state = JobState.PROGRESSING;
    });
    eb.consumer(addressPrefix + BUS_ADDR.GET_PROGRESS, message -> {
      String jobId = readJobId(message);
      log.debug("Received GET_PROGRESS message: jobId={}", jobId);
      JobItem job = jobItems.get(jobId);
      if(job==null){
        message.fail(-13, "Cannot find job with id=" + jobId);
      }else{
        message.reply(job.jobJO.copy().put("JOB_STATE", job.state));
      }
    });

    eb.consumer(addressPrefix + BUS_ADDR.DONE, message -> {
      log.debug("Received DONE message: {}", message.body());
      JobItem ji = workerEndedJob(message, JobState.DONE);
      
      String workerAddr = getWorkerAddress(message);
      sendJobsTo(workerAddr);

      if(ji.completionAddr!=null){
        log.debug("Notifying {} that job is done: {}", ji.completionAddr, ji.jobJO);
        vertx.eventBus().send(ji.completionAddr, ji.jobJO.copy());
      }
    });
    eb.consumer(addressPrefix + BUS_ADDR.FAIL, message -> {
      log.info("Received FAIL message: {}", message.body());
      JobItem ji = workerEndedJob(message, JobState.AVAILABLE);
      incrementFailCount(ji);

      String workerAddr = getWorkerAddress(message);
      sendJobsTo(workerAddr);
      
      // TODO: add optional settable functor/Callable/Runnable to notify of job failure
    });

    log.info("Ready: " + this + " addressPrefix=" + addressPrefix);
  }

  private String readJobId(Message<Object> message) {
    String jobId = message.headers().get(JOBID_HEADERATTRIBUTE);
    if(jobId==null && (message.body() instanceof String)) 
      jobId = (String) message.body();
    return jobId;
  }

  public static final String JOB_FAILEDCOUNT_ATTRIBUTE = "_jobFailedCount";

  private void incrementFailCount(JobItem ji) {
    Integer count = ji.jobJO.getInteger(JOB_FAILEDCOUNT_ATTRIBUTE, Integer.valueOf(0));
    ji.jobJO.put(JOB_FAILEDCOUNT_ATTRIBUTE, count.intValue() + 1);
  }

  private void sendJobsToNextIdleWorker() {
    String idleWorker = idleWorkers.peek();
    if (idleWorker != null) {
      sendJobsTo(idleWorker);
    }
  }

  private void sendJobsTo(String workerAddr) {
    if(!idleWorkers.remove(workerAddr))
      log.error("Could not remove {} from idleWorkers={}", workerAddr, idleWorkers);
    final JsonArray jobList = getAvailableJobs();
    log.debug("available jobs={}", jobList);
    vertx.eventBus().send(workerAddr, jobList, selectedJobReply -> {
      if (selectedJobReply.failed()) {
        log.error("selectedJobReply ", selectedJobReply.cause());
      } else if (selectedJobReply.result().body() == null) {
        if (jobList.size() > 0){
          log.info("Worker did not choose a job: {}", jobList);
        }
        if(!idleWorkers.add(workerAddr))
          log.error("Could not add {} to idleWorkers={}", workerAddr, idleWorkers);
        log.info("idleWorkers={}", idleWorkers);
        // jobItems may have changed by the time this reply is received
        final JsonArray jobList2 = getAvailableJobs();
        if(!jobList.equals(jobList2)){
          log.info("jobList has since changed; sending updated jobList to {}", workerAddr);
          sendJobsTo(workerAddr);
        }
      } else {
        workerStartedJob(selectedJobReply.result());
      }
    });
  }

  private JsonArray getAvailableJobs() {
    final JsonArray jobList = new JsonArray();
    jobItems.forEach((k, v) -> {
      if (v.state == JobState.AVAILABLE) {
        jobList.add(v.jobJO.copy());
      }
    });
    return jobList;
  }

  private JobItem workerStartedJob(Message<?> selectedJobMsg) {
    JsonObject jobJO = (JsonObject) selectedJobMsg.body();
    JobItem job = getJobItem(jobJO);

    String workerAddr = getWorkerAddress(selectedJobMsg);
    log.debug("Started job: worker={} on jobId={}", workerAddr, job.jobId);
    job.mergeIn(jobJO);
    job.state = JobState.STARTED;
    return job;
  }

  private JobItem workerEndedJob(Message<?> jobMsg, JobState newState) {
    JsonObject jobJO = (JsonObject) jobMsg.body();
    JobItem job = getJobItem(jobJO);

    String workerAddr = getWorkerAddress(jobMsg);
    idleWorkers.add(workerAddr);
    job.mergeIn(jobJO);
    job.state = newState;
    return job;
  }

  private JobItem getJobItem(JsonObject jobJO) {
    String jobId = JobItem.getJobId(jobJO);
    JobItem job = jobItems.get(jobId);
    checkNotNull(job, "Cannot find job with id=" + jobId);
    return job;
  }

  ////

  protected static class JobItem {

    enum JobState {
      AVAILABLE, STARTED, PROGRESSING, DONE
    };

    public static String getJobId(JsonObject jobJO) {
      return jobJO.getString(JOBID);
    }

    JobItem.JobState state;
    final String jobId;
    final String completionAddr;
    //String workerAddr;
    final JsonObject jobJO;

    JobItem(Message<?> message) {
      jobId = message.headers().get(JOBID_HEADERATTRIBUTE);
      checkNotNull(jobId);
      completionAddr = message.headers().get(JOB_COMPLETE_ADDRESS);
      jobJO = ((JsonObject) message.body()).copy();

      String existingJobId = jobJO.getString(JOBID);
      if (existingJobId == null)
        jobJO.put(JOBID, jobId);
      else if (!existingJobId.equals(jobId))
        throw new IllegalArgumentException("Job should not have attribute: " + JOBID);
    }

    public void mergeIn(JsonObject job) {
      jobJO.mergeIn(job);
    }

  }

  public static final String JOBID = "_jobId";

  private static final String JOBID_HEADERATTRIBUTE = "jobId";

  private static final String JOB_COMPLETE_ADDRESS = "jobCompleteAddress";

  public static DeliveryOptions createProducerHeader(String jobId, String jobCompletionAddress) {
    DeliveryOptions opts = new DeliveryOptions();
    opts.addHeader(JOBID_HEADERATTRIBUTE, jobId);
    if (jobCompletionAddress != null)
      opts.addHeader(JOB_COMPLETE_ADDRESS, jobCompletionAddress);
    return opts;
  }

  private static final String WORKER_ADDRESS = "workerAddress";

  public static DeliveryOptions createWorkerHeader(String workerAddress) {
    return new DeliveryOptions().addHeader(WORKER_ADDRESS, workerAddress);
  }

  public static String getWorkerAddress(Message<?> message) {
    return message.headers().get(WORKER_ADDRESS);
  }

}
