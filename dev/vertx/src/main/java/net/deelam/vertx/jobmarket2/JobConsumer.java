package net.deelam.vertx.jobmarket2;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.BeanJsonCodec;
import net.deelam.vertx.PojoTypeInfoMixIn;
import net.deelam.vertx.VerticleUtils;
import net.deelam.vertx.jobmarket2.JobMarket.BUS_ADDR;

/**
 * 
 * @author dd
 */
@Slf4j
@RequiredArgsConstructor
@ToString
public class JobConsumer extends AbstractVerticle {
  private final String serviceType;
  private final String jobType;
  private DeliveryOptions deliveryOptions;

  private String jmPrefix = null;

  @Override
  public void start() throws Exception {
    String myAddr = deploymentID();
    log.info("Ready: deploymentID={} this={}", deploymentID(), this);
    
    EventBus eb = vertx.eventBus();
    PojoTypeInfoMixIn.register();
    BeanJsonCodec.register(eb, JobDTO.class);
    BeanJsonCodec.register(eb, JobListDTO.class);

    deliveryOptions = JobMarket.createWorkerHeader(myAddr, jobType);

    eb.consumer(myAddr, jobListHandler);

    VerticleUtils.announceClientType(vertx, serviceType, msg -> {
      jmPrefix = msg.body();
      log.info("Sending client registration to {} from {}", jmPrefix, myAddr);
      vertx.eventBus().send(jmPrefix, null, deliveryOptions);
    });
  }
  
  public boolean isReady(){
    return jmPrefix!=null;
  }

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

  public JobConsumer(String svcType, String jobType, Function<JobDTO, Boolean> worker) {
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
  
  @Setter
  private Function<JobListDTO,JobDTO> jobPicker= dto -> {
    log.debug("jobs={}", dto);
    JobDTO picked=null;
    if (dto.jobs.size() > 0) {
      picked = dto.jobs.get(0);
    }
    StringBuilder jobsSb=new StringBuilder();
    dto.jobs.forEach( j -> jobsSb.append(" "+j.getId()));
    log.info("pickedJob={} from jobs={}", picked, jobsSb);
    return picked;
  };


  private ExecutorService threadPool = Executors.newCachedThreadPool();
  @Override
  public void stop() throws Exception {
    threadPool.shutdown(); // threads are not daemon threads, so will not die until timeout or shutdown()
    super.stop();
  }
  

  private static final String IS_PARTLY_DONE = "isPartlyDone";

  @Setter
  private Handler<Message<JobListDTO>> jobListHandler = msg -> {
    try {
      checkState(pickedJob == null, "Job in progress! " + pickedJob);
      JobListDTO jobs = msg.body();
      pickedJob = jobPicker.apply(jobs);
    } finally {
      // reply immediately so conversation doesn't timeout
      msg.reply(pickedJob, deliveryOptions); // must reply even if picked==null
    }

    if (pickedJob != null) {
      threadPool.execute(() -> {
        try {
          if (worker.apply(pickedJob)) {
//FIXME:            if (pickedJob.getParams().getBoolean(IS_PARTLY_DONE, false).booleanValue())
//              jobPartlyDone(); // creates new conversation
//            else
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

}
