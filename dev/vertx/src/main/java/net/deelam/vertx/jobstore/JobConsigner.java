package net.deelam.vertx.jobstore;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.TreeMap;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author dd
 */
@Slf4j
@NoArgsConstructor
public class JobConsigner extends AbstractVerticle {

  @Getter
  private String addressPrefix;

  public JobConsigner(String addressPrefix) {
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

  public static final String ADDJOB_ADDRESS_SUFFIX = "-addJob";
  public static final String LISTJOBS_ADDRESS_SUFFIX = "-listJobs";
  public static final String UPDATEJOB_ADDRESS_SUFFIX = "-updateJob";

  private static final Object OK_REPLY = null;

  // don't need to synchronize Set since Vert.x ensures single thread
  TreeMap<String, JobJO> jobs = new TreeMap<>();

  @Override
  public void start() throws Exception {
    initAddressPrefix();

    EventBus eb = vertx.eventBus();
    eb.consumer(addressPrefix + ADDJOB_ADDRESS_SUFFIX, message -> {
      log.info("Received addJob message: " + message.body());
      JobJO job = JobJO.wrap(message);
      String id = job.getJobId();
      {
        jobs.put(id, job);
        message.reply(OK_REPLY); //new JsonObject().put(JOB_ADDED_ATTRIBUTE, true));
      }
    });

    eb.consumer(addressPrefix + LISTJOBS_ADDRESS_SUFFIX, message -> {
      JsonArray jobList = new JsonArray();
      jobs.forEach((k, v) -> {
        if(!isJobStarted(v))
          jobList.add(v.copy());
      });
      message.reply(jobList);
    });

    eb.consumer(addressPrefix + UPDATEJOB_ADDRESS_SUFFIX, message -> {
      JobJO job = JobJO.wrap(message);
      String id = job.getJobId();
      {
        log.info("updateJob: {}", job);
        JobJO existingJob = jobs.get(id);
        String updateType=job.getUpdateType();
        switch(updateType){
          case JOB_STARTED_ATTRIBUTE:
            existingJob.remove(JOB_FAILED_ATTRIBUTE);
            break;
          case JOB_FAILED_ATTRIBUTE:
            incrementFailedCount(existingJob);
            existingJob.remove(JOB_STARTED_ATTRIBUTE);
            break;
          case JOB_COMPLETED_ATTRIBUTE:
            jobs.remove(id);
            String jobCompletionAddress = existingJob.getJobCompletionAddress();
            if(jobCompletionAddress!=null){
              log.info("Notifying jobCompletion to {}", jobCompletionAddress);
              eb.send(jobCompletionAddress, existingJob);
            }
            break;
          case "":
            break;
          default:
            throw new UnsupportedOperationException("updateType="+updateType);
        }
        existingJob.updateState(job);
        message.reply(OK_REPLY);
      }
    });

    log.info("Ready: " + this + " addressPrefix=" + addressPrefix);
  }

  ////

  public static final String JOB_STARTED_ATTRIBUTE = "jobStarted";

  private static boolean isJobStarted(JsonObject job) {
    return job.getBoolean(JOB_STARTED_ATTRIBUTE, false);
  }

  public static final String JOB_COMPLETED_ATTRIBUTE = "jobCompleted";

  public static final String JOB_FAILED_ATTRIBUTE = "jobFailed";

  public static final String JOB_FAILEDCOUNT_ATTRIBUTE = "jobFailedCount";

  private void incrementFailedCount(JsonObject existingJob) {
    Integer count = existingJob.getInteger(JOB_FAILEDCOUNT_ATTRIBUTE, Integer.valueOf(0));
    existingJob.put(JOB_FAILEDCOUNT_ATTRIBUTE, count.intValue() + 1);
  }

  ////

  public JobConsumer createJobConsumer() {
    return new JobConsumer(addressPrefix + LISTJOBS_ADDRESS_SUFFIX, addressPrefix + UPDATEJOB_ADDRESS_SUFFIX);
  }

  public JobProducer createJobProducer() {
    return new JobProducer(addressPrefix + ADDJOB_ADDRESS_SUFFIX);
  }

  ////

}
