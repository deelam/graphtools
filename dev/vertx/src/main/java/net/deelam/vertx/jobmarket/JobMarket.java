package net.deelam.vertx.jobmarket;

import static com.google.common.base.Preconditions.checkNotNull;

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
    REGISTER, UNREGISTER, SET_PROGRESS, DONE, FAIL // for workers
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
      if (!idleWorkers.add(workerAddr))
        log.error("Could not add {} to idleWorkers={}", workerAddr, idleWorkers);
      asyncSendJobsToNextIdleWorker();
    });
    eb.consumer(addressPrefix + BUS_ADDR.UNREGISTER, message -> {
      String workerAddr = getWorkerAddress(message);
      log.debug("Received UNREGISTER message from {}", workerAddr);
      if (!idleWorkers.remove(workerAddr))
        log.error("Could not remove {} from idleWorkers={}", workerAddr, idleWorkers);
    });

    eb.consumer(addressPrefix + BUS_ADDR.ADD_JOB, message -> {
      log.debug("Received ADD_JOB message: {}", message.body());
      JobItem ji = new JobItem(message);
      ji.state = JobState.AVAILABLE;
      JobItem existingJI = jobItems.get(ji.jobId);
      boolean addJob=true;
      if (existingJI!=null) {
        switch(existingJI.state){
          case AVAILABLE:
          case DONE:
          case FAILED:
            log.info("Job with id=" + ji.jobId + " already exists but has state="+existingJI.state+".  Adding job again.");
            addJob=true;
            break;
          case STARTED:
          case PROGRESSING:
            message.fail(-11, "Job with id=" + ji.jobId + " already exists and has started!");
            addJob=false;
            break;
        }
      }
      if(addJob){
        message.reply(OK_REPLY);
        jobItems.put(ji.jobId, ji);
        asyncSendJobsToNextIdleWorker();
      }
    });
    eb.consumer(addressPrefix + BUS_ADDR.REMOVE_JOB, message -> {
      String jobId = readJobId(message);
      log.debug("Received REMOVE_JOB message: jobId={}", jobId);
      JobItem ji = jobItems.get(jobId);
      if(ji==null){
        message.fail(-12, "Cannot find job with id=" + jobId);
      } else {
        switch (ji.state) {
          case AVAILABLE:
          case DONE:
          case FAILED:
            jobItems.remove(jobId);
            message.reply(OK_REPLY);
            break;
          case STARTED:
          case PROGRESSING:
            message.fail(-121, "Cannot remove job id=" + jobId + " with state=" + ji.state);
        }
      }
    });

    eb.consumer(addressPrefix + BUS_ADDR.SET_PROGRESS, (Message<JsonObject> message) -> {
      log.debug("Received SET_PROGRESS message: {}", message.body());
      JobItem job = getJobItem(message.body());
      job.mergeIn(message.body());
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

    eb.consumer(addressPrefix + BUS_ADDR.DONE, (Message<JsonObject> message) -> {
      log.debug("Received DONE message: {}", message.body());
      JobItem ji = workerEndedJob(message, JobState.DONE);
      
      //String workerAddr = getWorkerAddress(message);
      asyncSendJobsToNextIdleWorker();//(workerAddr);

      if(ji.completionAddr!=null){
        log.debug("Notifying {} that job is done: {}", ji.completionAddr, ji.jobJO);
        vertx.eventBus().send(ji.completionAddr, ji.jobJO.copy());
      }
    });
    eb.consumer(addressPrefix + BUS_ADDR.FAIL, (Message<JsonObject> message) -> {
      log.info("Received FAIL message: {}", message.body());
      JsonObject jobJO = message.body();
      JobItem job = getJobItem(jobJO);
      int failCount = incrementFailCount(job);
      
      JobState endState;
      if(failCount>=job.retryLimit){
        endState=JobState.FAILED;
      }else{
        endState=JobState.AVAILABLE;
      }
      JobItem ji = workerEndedJob(message, endState);

      //String workerAddr = getWorkerAddress(message);
      asyncSendJobsToNextIdleWorker(); //(workerAddr);
      
      if(endState==JobState.FAILED && ji.failureAddr!=null){
        log.debug("Notifying {} that job failed: {}", ji.failureAddr, ji.jobJO);
        vertx.eventBus().send(ji.failureAddr, ji.jobJO.copy());
      }
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

  private int incrementFailCount(JobItem ji) {
    Integer count = ji.jobJO.getInteger(JOB_FAILEDCOUNT_ATTRIBUTE, Integer.valueOf(0));
    int newCount = count.intValue() + 1;
    ji.jobJO.put(JOB_FAILEDCOUNT_ATTRIBUTE, newCount);
    return newCount;
  }

  //negotiate with one worker at a time so workers don't choose the same job
  private boolean isNegotiating=false; 
  private void asyncSendJobsToNextIdleWorker() {
    if(!isNegotiating){
      isNegotiating=true; // make sure all code paths reset this to false
      log.info("idleWorkers={}", idleWorkers);
      String idleWorker = idleWorkers.peek();
      if (idleWorker == null) // no workers available
        isNegotiating=false;
      else
        asyncSendJobsTo(idleWorker);
    }
  }

  private void asyncSendJobsTo(String workerAddr) {
    final JsonArray jobList = getAvailableJobs();
    log.debug("available jobs={}", jobList);
    if(jobList.size()==0){  // no jobs available
      isNegotiating=false;
    } else {
      vertx.eventBus().send(workerAddr, jobList, (AsyncResult<Message<JsonObject>> selectedJobReply) -> {
        if (selectedJobReply.failed()) {
          log.error("selectedJobReply failed: {}.  Removing worker={} permanently -- have worker register again if appropriate", 
              workerAddr, selectedJobReply.cause());
          if (!idleWorkers.remove(workerAddr))
            log.error("Could not remove {} from idleWorkers={}", workerAddr, idleWorkers);
        } else if (selectedJobReply.succeeded()) {
          if (selectedJobReply.result().body() == null) {
            if (jobList.size() > 0) {
              log.info("Worker did not choose a job: {}", jobList);
            }

            /*          // jobItems may have changed by the time this reply is received
                      final JsonArray jobList2 = getAvailableJobs();
                      if(!jobList.equals(jobList2)){
            log.info("jobList has since changed; sending updated jobList to {}", workerAddr);
            sendJobsTo(workerAddr);
                      }
            */
          } else {
            workerStartedJob(selectedJobReply.result());
          }
        }
        
        isNegotiating=false;
        asyncSendJobsToNextIdleWorker();
      });
    }
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

  private JobItem workerStartedJob(Message<JsonObject> selectedJobMsg) {
    JsonObject jobJO = (JsonObject) selectedJobMsg.body();
    JobItem job = getJobItem(jobJO);

    String workerAddr = getWorkerAddress(selectedJobMsg);
    log.debug("Started job: worker={} on jobId={}", workerAddr, job.jobId);
    if (!idleWorkers.remove(workerAddr))
      log.error("Could not remove {} from idleWorkers={}", workerAddr, idleWorkers);
    
    job.mergeIn(jobJO);
    job.state = JobState.STARTED;
    return job;
  }

  private JobItem workerEndedJob(Message<JsonObject> jobMsg, JobState newState) {
    JobItem job = getJobItem(jobMsg.body());

    String workerAddr = getWorkerAddress(jobMsg);
    if (!idleWorkers.add(workerAddr))
      log.error("Could not add {} to idleWorkers={}", workerAddr, idleWorkers);
    
    job.mergeIn(jobMsg.body());
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
      AVAILABLE, STARTED, PROGRESSING, DONE, FAILED
    };

    public static String getJobId(JsonObject jobJO) {
      return jobJO.getString(JOBID);
    }

    JobItem.JobState state;
    final String jobId;
    final String completionAddr;
    final String failureAddr;
    //String workerAddr;
    final JsonObject jobJO;
    final int retryLimit; // 0 means don't retry

    JobItem(Message<?> message) {
      jobId = message.headers().get(JOBID_HEADERATTRIBUTE);
      checkNotNull(jobId);
      completionAddr = message.headers().get(JOB_COMPLETE_ADDRESS);
      failureAddr = message.headers().get(JOB_FAILURE_ADDRESS);
      jobJO = ((JsonObject) message.body()).copy();

      String retryLimitStr = message.headers().get(JOB_RETRY_LIMIT);
      if(retryLimitStr==null)
        retryLimit=0; 
      else
        retryLimit=Integer.parseInt(retryLimitStr);
      

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
  
  private static final String JOB_FAILURE_ADDRESS = "jobFailureAddress";
  
  private static final String JOB_RETRY_LIMIT = "jobRetryLimit";

  public static DeliveryOptions createProducerHeader(String jobId, String jobCompletionAddress) {
    return createProducerHeader(jobId, jobCompletionAddress, null, 0);
  }
  
  public static DeliveryOptions createProducerHeader(String jobId, String jobCompletionAddress, String jobFailureAddress, int jobRetryLimit) {
    DeliveryOptions opts = new DeliveryOptions();
    opts.addHeader(JOBID_HEADERATTRIBUTE, jobId);
    if (jobCompletionAddress != null)
      opts.addHeader(JOB_COMPLETE_ADDRESS, jobCompletionAddress);
    if (jobFailureAddress != null)
      opts.addHeader(JOB_FAILURE_ADDRESS, jobFailureAddress);
    opts.addHeader(JOB_RETRY_LIMIT, Integer.toString(jobRetryLimit));
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