package net.deelam.vertx.jobmarket;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import io.vertx.core.AbstractVerticle;
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
 * -addJob(id,completionAddr,job)
 * -getProgress(id)
 * 
 * -register(workerAddr)
 * -setProgress(job)
 * -done(job)
 * -fail(job)
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

  enum BUS_ADDR {
    ADD_JOB, GET_PROGRESS, // for producers 
    REGISTER, SET_PROGRESS, DONE, FAIL // for workers
  };

  private static final Object OK_REPLY = null;

  // jobId -> JobItem
  private Map<String, JobItem> jobItems = new HashMap<>();
  
  private Queue<String> idleWorkers = new LinkedList<>();

  @Override
  public void start() throws Exception {
    initAddressPrefix();

    EventBus eb = vertx.eventBus();
    eb.consumer(addressPrefix + BUS_ADDR.REGISTER, message -> {
      String workerAddr = getWorkerAddress(message);
      log.info("Received REGISTER message from " + workerAddr);
      idleWorkers.add(workerAddr);
      sendJobsTo(workerAddr);
    });

    eb.consumer(addressPrefix + BUS_ADDR.ADD_JOB, message -> {
      log.info("Received ADD_JOB message: {}", message.body());
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

    eb.consumer(addressPrefix + BUS_ADDR.SET_PROGRESS, message -> {
      log.info("Received SET_PROGRESS message: {}", message.body());
      JsonObject jobJO = (JsonObject) message.body();
      JobItem job = getJobItem(jobJO);
      job.mergeIn(jobJO);
      job.state = JobState.PROGRESSING;
    });
    eb.consumer(addressPrefix + BUS_ADDR.GET_PROGRESS, message -> {
      log.info("Received GET_PROGRESS message: {}", message.body());
      String jobId = message.headers().get(JOBID_ATTRIBUTE);
      //String jobId = (String) message.body();
      JobItem job = jobItems.get(jobId);
      checkNotNull(job, "Cannot find job with id=" + jobId);
      message.reply(job.jobJO.copy().put("JOB_STATE", job.state));
    });

    eb.consumer(addressPrefix + BUS_ADDR.DONE, message -> {
      log.info("Received DONE message: {}", message.body());
      JobItem ji = workerEndedJob(message, JobState.DONE);
      String workerAddr = getWorkerAddress(message);
      sendJobsTo(workerAddr);

      vertx.eventBus().send(ji.completionAddr, ji.jobJO);
    });
    eb.consumer(addressPrefix + BUS_ADDR.FAIL, message -> {
      log.info("Received FAIL message: {}", message.body());
      JobItem ji = workerEndedJob(message, JobState.AVAILABLE);
      incrementFailCount(ji);

      String workerAddr = getWorkerAddress(message);
      sendJobsTo(workerAddr);
    });

    log.info("Ready: " + this + " addressPrefix=" + addressPrefix);
  }

  public static final String JOB_FAILEDCOUNT_ATTRIBUTE = "_jobFailedCount";

  private void incrementFailCount(JobItem ji) {
    Integer count = ji.jobJO.getInteger(JOB_FAILEDCOUNT_ATTRIBUTE, Integer.valueOf(0));
    ji.jobJO.put(JOB_FAILEDCOUNT_ATTRIBUTE, count.intValue() + 1);
  }

  private void sendJobsToNextIdleWorker() {
    String idleCons = idleWorkers.peek();
    if (idleCons != null) {
      sendJobsTo(idleCons);
    }
  }

  private void sendJobsTo(String workerAddr) {
    final JsonArray jobList = new JsonArray();
    jobItems.forEach((k, v) -> {
      if (v.state == JobState.AVAILABLE) {
        jobList.add(v.jobJO.copy());
      }
    });
    log.info("jobList=" + jobList);
    {
      vertx.eventBus().send(workerAddr, jobList, selectedJobReply -> {
        if (selectedJobReply.failed()) {
          log.error("selectedJobReply ", selectedJobReply.cause());
        } else if (selectedJobReply.result().body() == null) {
          if (jobList.size() > 0)
            log.info("Worker did not choose a job: {}", jobList);
        } else {
          workerStartedJob(selectedJobReply.result());
        }
      });
    }
  }

  private JobItem workerStartedJob(Message<?> selectedJobMsg) {
    JsonObject jobJO = (JsonObject) selectedJobMsg.body();
    JobItem job = getJobItem(jobJO);

    String workerAddr = getWorkerAddress(selectedJobMsg);
    idleWorkers.remove(workerAddr);
    log.info("STARTED {} on {}", workerAddr, job.jobId);
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

    private static final String JOBID = "_jobId";

    public static String getJobId(JsonObject jobJO) {
      return jobJO.getString(JOBID);
    }

    JobItem.JobState state;
    final String jobId;
    final String completionAddr;
    //String workerAddr;
    final JsonObject jobJO;

    JobItem(Message<?> message) {
      jobId = message.headers().get(JOBID_ATTRIBUTE);
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

  private static final String JOBID_ATTRIBUTE = "jobId";

  private static final String JOB_COMPLETE_ADDRESS = "jobCompleteAddress";

  public static DeliveryOptions createProducerHeader(String jobId, String jobCompletionAddress) {
    DeliveryOptions opts = new DeliveryOptions();
    opts.addHeader(JOBID_ATTRIBUTE, jobId);
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
