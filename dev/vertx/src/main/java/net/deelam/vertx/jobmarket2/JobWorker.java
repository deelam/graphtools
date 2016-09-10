package net.deelam.vertx.jobmarket2;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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
@Slf4j
@RequiredArgsConstructor
@ToString
public class JobWorker extends AbstractVerticle {
  private final String serviceType;
  private final String jobType;
  private DeliveryOptions deliveryOptions;

  private String jmPrefix = null;

  @Override
  public void start() throws Exception {
    String myAddr = deploymentID();
    log.info("JobWorker ready: myAddr={}: {}", myAddr, this.getClass());
    deliveryOptions = JobMarket.createWorkerHeader(myAddr, jobType);

    vertx.eventBus().consumer(myAddr, jobListHandler);

    VerticleUtils.announceClientType(vertx, serviceType, msg -> {
      jmPrefix = msg.body();
      log.debug("Sending client registration to {} from {}", jmPrefix, myAddr);
      vertx.eventBus().send(jmPrefix, null, deliveryOptions);
    });
  }

  //  @Deprecated
  //  public void register() {
  //    vertx.eventBus().send(jmPrefix + BUS_ADDR.REGISTER, null, deliveryOptions);
  //  }

  private JobDTO pickedJob = null;

  public void sendJobProgress() {
    checkNotNull(pickedJob, "No job picked!");
    vertx.eventBus().send(jmPrefix + BUS_ADDR.SET_PROGRESS, pickedJob, deliveryOptions);
  }

  public void jobPartlyDone() {
    checkNotNull(pickedJob, "No job picked!");
    JobDTO prevJob = pickedJob;
    pickedJob = null; // set to null before notifying jobMarket, which will offer more jobs
    vertx.eventBus().send(jmPrefix + BUS_ADDR.PARTLY_DONE, prevJob, deliveryOptions);
  }

  public void jobDone() {
    checkNotNull(pickedJob, "No job picked!");
    JobDTO prevJob = pickedJob;
    pickedJob = null; // set to null before notifying jobMarket, which will offer more jobs
    vertx.eventBus().send(jmPrefix + BUS_ADDR.DONE, prevJob, deliveryOptions);
  }

  public void jobFailed() {
    checkNotNull(pickedJob, "No job picked!");
    JobDTO prevJob = pickedJob;
    pickedJob = null; // set to null before notifying jobMarket, which will offer more jobs
    vertx.eventBus().send(jmPrefix + BUS_ADDR.FAIL, prevJob, deliveryOptions);
  }

  public JobWorker(String svcType, String jobType, Function<JobDTO, Boolean> worker) {
    this(svcType, jobType);
    setWorker(worker);
  }

  @Setter
  private Function<JobDTO, Boolean> worker = new Function<JobDTO, Boolean>() {
    @Override
    public Boolean apply(JobDTO job) {
      log.info("TODO: Do work on: {}", job);
      return true;
    }
  };

  private ExecutorService threadPool = Executors.newCachedThreadPool();

  public static final String IS_PARTLY_DONE = "isPartlyDone";

  @Setter
  private Handler<Message<JobListDTO>> jobListHandler = msg -> {
    try {
      checkState(pickedJob == null, "Job in progress! " + pickedJob);
      JobListDTO jobs = msg.body();
      pickedJob = pickJob(jobs);
    } finally {
      // reply immediately so conversation doesn't timeout
      msg.reply(pickedJob, deliveryOptions); // must reply even if picked==null
    }

    if (pickedJob != null) {
      threadPool.execute(() -> {
        try {
          if (worker.apply(pickedJob)) {
            if (pickedJob.getParams().getBoolean(IS_PARTLY_DONE, false).booleanValue())
              jobPartlyDone(); // creates new conversation
            else
              jobDone(); // creates new conversation
          } else {
            jobFailed(); // creates new conversation
          }
        } catch (Exception | Error e) {
          log.error("Worker " + worker + " threw exception; notifying job failed", e);
          jobFailed(); // creates new conversation
        }
      });
    }
  };

  protected JobDTO pickJob(JobListDTO jobList) {
    log.debug("jobs={}", jobList);
    JobDTO picked = null;
    if (jobList.getJobs().size() > 0) {
      picked = jobList.jobs.get(0);
    }

    StringBuilder jobsSb = jobList.jobs.stream().map(j -> j.getId() + " ")
        .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append);
    log.info("pickedJob={} from jobs={}", picked, jobsSb);
    return picked;
  }

}
