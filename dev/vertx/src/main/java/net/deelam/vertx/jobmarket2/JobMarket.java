package net.deelam.vertx.jobmarket2;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.BeanJsonCodec;
import net.deelam.vertx.PojoTypeInfoMixIn;
import net.deelam.vertx.VerticleUtils;
import net.deelam.vertx.jobmarket2.JobMarket.JobItem.JobState;

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
 * JobWorkers register and JobProducers addJobs.  For each of these events, a list of available jobs are sent to the next idle Worker to choose a job.
 * (If no job is chosen and the availableJobList has changed, the latest list is sent to the Worker.)
 * When a Worker is done or has failed the job, it is sent the latest availableJobList.
 * 
 * Workers can submit setProgress to update the job JsonObject.  
 * Calling done(job) will send a notification to the completionAddress set in the job JsonObject.
 * Upon completion, a job is simply marked as DONE.  To delete the job entry, call removeJob(id). 
 * 
 * If job fails, a failCount is incremented and the JOB_FAILEDCOUNT_ATTRIBUTE property set on the job JsonObject.
 * 
 * --
 * 
 * JobMarket negotiates with one worker at a time so multiple workers cannot choose the same job.
 * pickyWorkers holds workers who did not pick any existing job, and so will not be notified until a new job is added.
 * When a new job is added, all workers in pickyIdleWorkers will be added to idleWorkers.
 * 
 * @author dd
 */
@Slf4j
@ToString
public class JobMarket extends AbstractVerticle {
  private final String serviceType;

  @Getter
  private String addressBase;

  public JobMarket(String serviceType/*, String addressPrefix*/) {
    this.serviceType = serviceType;
    //this.addressBase = "JobMarket-"+System.currentTimeMillis(); //addressPrefix;
  }

  public static final String EVENT_BUS_PREFIX = "eventBusPrefix";

  private void initAddressPrefix() {
    final String configAddrPrefix = config().getString(EVENT_BUS_PREFIX);
    if (addressBase == null) {
      addressBase = configAddrPrefix;
      if (addressBase == null) {
        addressBase = deploymentID();
        log.info("Using deploymentID() as addressBase={}", addressBase);
      }
    } else if (configAddrPrefix != null) {
      log.warn("Ignoring {} configuration since already set (in constructor): {}", EVENT_BUS_PREFIX, addressBase);
    }

    checkNotNull(addressBase, "Must set '" + EVENT_BUS_PREFIX + "' config since not provided in constructor!");
  }

  public enum BUS_ADDR {
    ADD_JOB, REMOVE_JOB, GET_PROGRESS, // for producers 
    /*REGISTER,*/ UNREGISTER, SET_PROGRESS, DONE, PARTLY_DONE, FAIL // for workers
  };

  private static final Object OK_REPLY = "ACK";

  // jobId -> JobItem
  private Map<String, JobItem> jobItems = new LinkedHashMap<>();

  private HashMap<String,Worker> knownWorkers = new HashMap<>();
  private LinkedHashSet<String> idleWorkers = new LinkedHashSet<>();
  private LinkedHashSet<String> pickyWorkers = new LinkedHashSet<>();

  @SuppressWarnings("unused")
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
    PojoTypeInfoMixIn.register();
    BeanJsonCodec.register(eb, JobDTO.class);
    BeanJsonCodec.register(eb, JobListDTO.class);

    eb.consumer(addressBase/* + BUS_ADDR.REGISTER*/, message -> {
      String workerAddr = getWorkerAddress(message);
      String workerType = getWorkerJobType(message);
      log.debug("Received initial message from {}", workerAddr);
      if(workerType==null){
        log.error("Cannot register worker with null type: {}", workerAddr);
        return;
      }
      
      if (knownWorkers.containsKey(workerAddr))
        log.info("Worker already registered: {}", workerAddr);
      else 
        knownWorkers.put(workerAddr, new Worker(workerAddr, workerType));
      
      if (idleWorkers.contains(workerAddr))
        log.info("Worker already registered and is idle: {}", workerAddr);
      else if (!idleWorkers.add(workerAddr))
        log.error("Could not add {} to idleWorkers={}", workerAddr, idleWorkers);

      asyncNegotiateJobWith(workerAddr);
    });
    
    eb.consumer(addressBase + BUS_ADDR.UNREGISTER, message -> {
      String workerAddr = getWorkerAddress(message);
      log.debug("Received UNREGISTER message from {}", workerAddr);
      if (!idleWorkers.remove(workerAddr))
        log.error("Could not remove {} from idleWorkers={}", workerAddr, idleWorkers);
    });

    eb.consumer(addressBase + BUS_ADDR.ADD_JOB, (Message<JobDTO> message) -> {
      log.debug("Received ADD_JOB message: {}", message.body());
      JobItem ji = new JobItem(message);
      ji.state = JobState.AVAILABLE;
      JobItem existingJI = jobItems.get(ji.getId());
      boolean addJob = true;
      if (existingJI != null) {
        switch (existingJI.state) {
          case AVAILABLE:
          case DONE:
          case FAILED:
            log.info("Job with id=" + ji.getId() + " already exists but has state=" + existingJI.state
                + ".  Adding job again.");
            addJob = true;
            break;
          case STARTED:
          case PROGRESSING:
            message.fail(-11, "Job with id=" + ji.getId() + " already exists and has started!");
            addJob = false;
            break;
        }
      }
      if (addJob) {
        log.info("Adding job: {}", ji);
        message.reply(OK_REPLY);
        jobItems.put(ji.getId(), ji);

        jobAdded = true; // in case in the middle of negotiating

        log.debug("Moving all pickyWorkers {} to idleWorkers: {}", pickyWorkers, idleWorkers);
        for (Iterator<String> itr = pickyWorkers.iterator(); itr.hasNext();) {
          idleWorkers.add(itr.next());
          itr.remove();
        }
        asyncNegotiateJobWithNextIdleWorker();
      }
    });
    eb.consumer(addressBase + BUS_ADDR.REMOVE_JOB, message -> {
      String jobId = readJobId(message);
      log.debug("Received REMOVE_JOB message: jobId={}", jobId);
      JobItem ji = jobItems.get(jobId);
      if (ji == null) {
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

    eb.consumer(addressBase + BUS_ADDR.SET_PROGRESS, (Message<JobDTO> message) -> {
      log.debug("Received SET_PROGRESS message: {}", message.body());
      JobItem job = getJobItem(message.body());
      job.mergeIn(message.body());
      job.state = JobState.PROGRESSING;
    });
    eb.consumer(addressBase + BUS_ADDR.GET_PROGRESS, message -> {
      String jobId = readJobId(message);
      log.debug("Received GET_PROGRESS message: jobId={}", jobId);
      JobItem job = jobItems.get(jobId);
      if (job == null) {
        message.fail(-13, "Cannot find job with id=" + jobId);
      } else {
        JobDTO dto = job.jobJO.copy();
        //dto.getParams().put("JOB_STATE", job.state);
        message.reply(dto);
      }
    });

    eb.consumer(addressBase + BUS_ADDR.PARTLY_DONE, (Message<JobDTO> message) -> {
      // worker completed its part of the job
      log.debug("Received PARTLY_DONE message: {}", message.body());
      JobItem ji = workerEndedJob(message, JobState.AVAILABLE);
      log.debug("Partly done: {}", ji.jobJO);

      String workerAddr = getWorkerAddress(message);
      asyncNegotiateJobWith(workerAddr);
    });
    eb.consumer(addressBase + BUS_ADDR.DONE, (Message<JobDTO> message) -> {
      log.debug("Received DONE message: {}", message.body());
      JobItem ji = workerEndedJob(message, JobState.DONE);

      String workerAddr = getWorkerAddress(message);
      asyncNegotiateJobWith(workerAddr);

      if (ji.completionAddr != null) {
        log.debug("Notifying {} that job is done: {}", ji.completionAddr, ji.jobJO);
        vertx.eventBus().send(ji.completionAddr, ji.jobJO.copy());
      }
    });
    eb.consumer(addressBase + BUS_ADDR.FAIL, (Message<JobDTO> message) -> {
      log.info("Received FAIL message: {}", message.body());
      JobDTO jobJO = message.body();
      JobItem job = getJobItem(jobJO);
      int failCount = incrementFailCount(job);

      JobState endState;
      if (failCount >= job.retryLimit) {
        endState = JobState.FAILED;
      } else {
        endState = JobState.AVAILABLE;
      }
      JobItem ji = workerEndedJob(message, endState);

      String workerAddr = getWorkerAddress(message);
      asyncNegotiateJobWith(workerAddr);

      if (endState == JobState.FAILED && ji.failureAddr != null) {
        log.debug("Notifying {} that job failed: {}", ji.failureAddr, ji.jobJO);
        vertx.eventBus().send(ji.failureAddr, ji.jobJO.copy());
      }
    });

    // announce after setting eb.consumer
    VerticleUtils.announceServiceType(vertx, serviceType, addressBase);

    log.info("Ready: addressBase={} this={}", addressBase, this);
  }

  private String readJobId(Message<Object> message) {
    if (message.body() instanceof String)
      return (String) message.body();
    else
      throw new IllegalArgumentException("Cannot parse jobId from: "+message.body());
  }

  private static final String JOB_FAILEDCOUNT_ATTRIBUTE = "_jobFailedCount";

  private int incrementFailCount(JobItem ji) {
    ji.jobFailedCount+=1;
    return ji.jobFailedCount;
//    Integer count = ji.jobJO.getParams().getInteger(JOB_FAILEDCOUNT_ATTRIBUTE, Integer.valueOf(0));
//    int newCount = count.intValue() + 1;
//    ji.jobJO.getParams().put(JOB_FAILEDCOUNT_ATTRIBUTE, newCount);
//    return newCount;
  }

  //negotiate with one worker at a time so workers don't choose the same job
  private boolean isNegotiating = false;

  private void asyncNegotiateJobWithNextIdleWorker() {
    String idleWorker = Iterables.getFirst(idleWorkers, null);
    if (idleWorker == null) { // no workers available
      return;
    }
    asyncNegotiateJobWith(idleWorker);
  }
  
  private void asyncNegotiateJobWith(String idleWorker) {
    if (!isNegotiating) {
      final JobListDTO jobList = getAvailableJobsFor(idleWorker);
      if (jobList.jobs.size() == 0) { // no jobs available
        return;
      }
      
      log.info("Negotiating jobs with {}, idleWorkers={}", idleWorker, idleWorkers);
      isNegotiating = true; // make sure all code paths reset this to false
      asyncSendJobsTo(idleWorker, jobList);
    }
  }

  private boolean jobAdded = false;

  private void asyncSendJobsTo(final String workerAddr, final JobListDTO jobList) {
    jobAdded = false;
    
    log.debug("Sending to {} available jobs={}", workerAddr, jobList);
    DeliveryOptions delivOpt=new DeliveryOptions().setSendTimeout(10000L);
    vertx.eventBus().send(workerAddr, jobList, delivOpt, (AsyncResult<Message<JobDTO>> selectedJobReply) -> {
      //log.debug("reply from worker={}", selectedJobReply.result().headers().get(WORKER_ADDRESS));
      boolean negotiateWithNextIdle = true;

      if (selectedJobReply.failed()) {
        log.warn(
            "selectedJobReply failed: {}.  Removing worker={} permanently -- have worker register again if appropriate",
            workerAddr, selectedJobReply.cause());
        if (!idleWorkers.remove(workerAddr))
          log.error("Could not remove {} from idleWorkers={}", workerAddr, idleWorkers);
      } else if (selectedJobReply.succeeded()) {
        if (selectedJobReply.result().body() == null) {
          /*if (jobList.size() > 0) */ {
            log.debug("Worker {} did not choose a job: {}", workerAddr, toString(jobList));

            // jobItems may have changed by the time this reply is received
            if (jobAdded) {
              log.info("jobList has since changed; sending updated jobList to {}", workerAddr);
              JobListDTO availableJobs = getAvailableJobsFor(workerAddr);
              asyncSendJobsTo(workerAddr, availableJobs);
              negotiateWithNextIdle = false; // still negotiating with current idleWorker
            } else {
              log.debug("Moving idleWorker to pickyWorkers queue: {}", workerAddr);
              if (!idleWorkers.remove(workerAddr)) {
                log.error("Could not remove {} from idleWorkers={}", workerAddr, idleWorkers);
              } else {
                if (!pickyWorkers.add(workerAddr))
                  log.error("Could not add {} to pickyWorkers={}", workerAddr, pickyWorkers);
              }
            }
          }
        } else {
          workerStartedJob(selectedJobReply.result());
        }
      }

      if (negotiateWithNextIdle) {
        isNegotiating = false; // close current negotiation
        asyncNegotiateJobWithNextIdleWorker();
      }
    });
  }

  private String toString(JobListDTO jobList) {
    StringBuilder sb = new StringBuilder();
    jobList.getJobs().forEach(j -> sb.append("\n  ").append(j.id));
    return sb.toString();
  }

  private JobListDTO getAvailableJobsFor(String workerAddr) {
    final String jobType=knownWorkers.get(workerAddr).type;
    List<JobDTO> jobListL = jobItems.entrySet().stream().filter(e->{
      if (e.getValue().state != JobState.AVAILABLE)
        return false;
      return (jobType==null) || jobType.equals(e.getValue().jobJO.getType());
    }).map( e ->{
      JobItem ji = e.getValue();
      JobDTO dto = ji.jobJO.copy();
      return dto;
    }).collect(Collectors.toList());

    if(jobListL.size()==0)
      log.info("No '{}' jobs for: {}", jobType, workerAddr);
    
    JobListDTO jobList = new JobListDTO(jobListL);
    return jobList;
  }

  private JobItem workerStartedJob(Message<JobDTO> selectedJobMsg) {
    JobDTO jobJO = selectedJobMsg.body();
    JobItem job = getJobItem(jobJO);

    String workerAddr = getWorkerAddress(selectedJobMsg);
    log.debug("Started job: worker={} on jobId={}", workerAddr, job.getId());
    if (!idleWorkers.remove(workerAddr))
      log.error("Could not remove {} from idleWorkers={}", workerAddr, idleWorkers);

    job.mergeIn(jobJO);
    job.state = JobState.STARTED;
    return job;
  }

  private JobItem workerEndedJob(Message<JobDTO> jobMsg, JobState newState) {
    JobItem job = getJobItem(jobMsg.body());

    String workerAddr = getWorkerAddress(jobMsg);
    if (!idleWorkers.add(workerAddr))
      log.error("Could not add {} to idleWorkers={}", workerAddr, idleWorkers);

    job.mergeIn(jobMsg.body());
    job.state = newState;
    return job;
  }

  private JobItem getJobItem(JobDTO jobJO) {
    String jobId = jobJO.getId();
    JobItem job = jobItems.get(jobId);
    checkNotNull(job, "Cannot find job with id=" + jobId);
    return job;
  }

  ////
  
  @RequiredArgsConstructor
  private static class Worker {
    final String id;
    final String type;
  }

  @ToString
  protected static class JobItem {

    enum JobState {
      AVAILABLE, STARTED, PROGRESSING, DONE, FAILED
    };

    JobItem.JobState state;
    final String completionAddr;
    final String failureAddr;
    final int retryLimit; // 0 means don't retry
    int jobFailedCount=0;
    final JobDTO jobJO;

    JobItem(Message<JobDTO> message) {
      completionAddr = message.headers().get(JOB_COMPLETE_ADDRESS);
      failureAddr = message.headers().get(JOB_FAILURE_ADDRESS);
      jobJO = ((JobDTO) message.body()).copy();

      String retryLimitStr = message.headers().get(JOB_RETRY_LIMIT);
      if (retryLimitStr == null)
        retryLimit = 0;
      else
        retryLimit = Integer.parseInt(retryLimitStr);


      checkNotNull(jobJO.id);
      
//      String existingJobId = jobJO.id;
//      if (existingJobId == null)
//        jobJO.id=jobId;
//      else if (!existingJobId.equals(jobId))
//        throw new IllegalArgumentException("Job's jobId attribute: "+jobJO.id+" != (msgHeader's jobIdAttrib="+jobId+")");
    }

    public String getId() {
      return jobJO.id;
    }

    public void mergeIn(JobDTO job) {
//      if(job.params!=null)
//        jobJO.getParams().mergeIn(job.params);
    }

  }

  private static final String JOB_COMPLETE_ADDRESS = "jobCompleteAddress";

  private static final String JOB_FAILURE_ADDRESS = "jobFailureAddress";

  private static final String JOB_RETRY_LIMIT = "jobRetryLimit";

  public static DeliveryOptions createProducerHeader(String jobCompletionAddress) {
    return createProducerHeader(jobCompletionAddress, null, 0);
  }

  public static DeliveryOptions createProducerHeader(String jobCompletionAddress,
      String jobFailureAddress, int jobRetryLimit) {
    DeliveryOptions opts = new DeliveryOptions();
    if (jobCompletionAddress != null)
      opts.addHeader(JOB_COMPLETE_ADDRESS, jobCompletionAddress);
    if (jobFailureAddress != null)
      opts.addHeader(JOB_FAILURE_ADDRESS, jobFailureAddress);
    opts.addHeader(JOB_RETRY_LIMIT, Integer.toString(jobRetryLimit));
    return opts;
  }

  private static final String WORKER_ADDRESS = "workerAddress";
  private static final String WORKER_JOBTYPE = "workerJobType";

  public static DeliveryOptions createWorkerHeader(String workerAddress, String workerJobType) {
    DeliveryOptions opts = new DeliveryOptions()
        .addHeader(WORKER_ADDRESS, workerAddress);
    if(workerJobType!=null)
        opts.addHeader(WORKER_JOBTYPE, workerJobType);
    return opts;
  }

  public static String getWorkerAddress(Message<?> message) {
    return message.headers().get(WORKER_ADDRESS);
  }

  public static String getWorkerJobType(Message<?> message) {
    return message.headers().get(WORKER_JOBTYPE);
  }

}
