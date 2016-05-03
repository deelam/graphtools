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
 * -register(consumerAddr)
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
    REGISTER, SET_PROGRESS, DONE, FAIL // for consumers
  };

  private static final Object OK_REPLY = null;
  
  // don't need to synchronize Set since Vert.x ensures single thread
//  TreeMap<String, JobJO> jobs = new TreeMap<>();

  // consumerAddr -> jobId
  private Map<String, String> consumers = new HashMap<>();

  // jobId -> JobItem
  private Map<String, JobItem> jobItems = new HashMap<>();

  private static final String IDLE_JOBID = "";
  
  private Queue<String> idleConsumers=new LinkedList<>();
  
  @Override
  public void start() throws Exception {
    initAddressPrefix();

    EventBus eb = vertx.eventBus();
    eb.consumer(addressPrefix + BUS_ADDR.REGISTER, message -> {
      String consumerAddr=getConsumerAddress(message);
      log.info("Received REGISTER message from " + consumerAddr);
      addIdleConsumer(consumerAddr);
      sendJobsTo(consumerAddr);
    });
    eb.consumer(addressPrefix + BUS_ADDR.ADD_JOB, message -> {
      log.info("Received ADD_JOB message: " + message);
      JobItem ji = new JobItem(message);
      ji.state=JobState.AVAILABLE;
      if(jobItems.containsKey(ji.jobId)){
        message.fail(-11, "Job with id="+ji.jobId+" already exists!");
      } else {
        jobItems.put(ji.jobId, ji);
        sendJobsToNextIdleConsumer();
      }
    });
    eb.consumer(addressPrefix + BUS_ADDR.DONE, message -> {
      log.info("Received DONE message: " + message);
      consumerEndedJob(message, JobState.DONE);
      String consumerAddr=getConsumerAddress(message);
      sendJobsTo(consumerAddr);
      
      JsonObject jobJO = (JsonObject) message.body();
      JobItem job = getJobItem(jobJO);
      vertx.eventBus().send(job.completionAddr, job.jobJO);
    });
    eb.consumer(addressPrefix + BUS_ADDR.FAIL, message -> {
      log.info("Received FAIL message: " + message);
      consumerEndedJob(message, JobState.AVAILABLE);
      String consumerAddr=getConsumerAddress(message);
      sendJobsTo(consumerAddr);
    });
    
    log.info("Ready: " + this + " addressPrefix=" + addressPrefix);
  }

  private void addIdleConsumer(String consumerAddr) {
    consumers.put(consumerAddr, IDLE_JOBID);
    idleConsumers.add(consumerAddr);
  }
  private void removeIdleConsumer(String consumerAddr, JobItem job) {
    consumers.put(consumerAddr, job.jobId);
    idleConsumers.remove(consumerAddr);
  }

  private void sendJobsToNextIdleConsumer() {
    String idleCons = idleConsumers.peek();
    if(idleCons!=null){
      sendJobsTo(idleCons);
    }
  }

  private void sendJobsTo(String consumerAddr) {
    JsonArray jobList = new JsonArray();
    jobItems.forEach((k, v) -> {
      if (v.state==JobState.AVAILABLE){
        jobList.add(v.jobJO.copy());
      }
    });
    log.info("jobList="+jobList);
    if(jobList.size()>0){
      vertx.eventBus().send(consumerAddr, jobList, selectedJobReply -> {
        if(selectedJobReply.failed())
          log.error("selectedJobReply ", selectedJobReply.cause());
        else 
          consumerStartedJob(selectedJobReply.result());
      });
    }
  }

  private void consumerStartedJob(Message<?> selectedJobMsg) {
    String consumerAddr = getConsumerAddress(selectedJobMsg);
    JsonObject jobJO = (JsonObject) selectedJobMsg.body();
    JobItem job = getJobItem(jobJO);
    log.info("STARTED {} on {}",consumerAddr, job.jobId);
    {
      removeIdleConsumer(consumerAddr, job);
      job.mergeIn(jobJO);
      job.state=JobState.STARTED;
    }
  }

  private void consumerEndedJob(Message<?> jobMsg, JobState newState) {
    JsonObject jobJO = (JsonObject) jobMsg.body();
    JobItem job = getJobItem(jobJO);
    {
      String consumerAddr = getConsumerAddress(jobMsg);
      addIdleConsumer(consumerAddr);
      job.mergeIn(jobJO);
      job.state=newState;
    }
  }

  private JobItem getJobItem(JsonObject jobJO) {
    String jobId = JobItem.getJobId(jobJO);
    JobItem job = jobItems.get(jobId);
    checkNotNull(job, "Cannot find job with id="+jobId);
    return job;
  }
  
  ////
  
  public static final String JOB_FAILEDCOUNT_ATTRIBUTE = "jobFailedCount";

  ////

  static class JobItem {

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
    //String consumerAddr;
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

  public static final String JOB_COMPLETE_ADDRESS = "jobCompleteAddress";

  public static DeliveryOptions createProducerHeader(String jobId, String jobCompletionAddress) {
    DeliveryOptions opts = new DeliveryOptions();
    opts.addHeader(JOBID_ATTRIBUTE, jobId);
    if (jobCompletionAddress != null)
      opts.addHeader(JOB_COMPLETE_ADDRESS, jobCompletionAddress);
    return opts;
  }

  public static final String CONSUMER_ADDRESS = "consumerAddress";

  public static DeliveryOptions createConsumerHeader(String consumerAddress) {
    return new DeliveryOptions().addHeader(CONSUMER_ADDRESS, consumerAddress);
  }
  
  public static String getConsumerAddress(Message<?> message) {
    return message.headers().get(CONSUMER_ADDRESS);
  }

}
