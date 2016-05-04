package net.deelam.vertx.jobmarket;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.jobmarket.JobMarket.BUS_ADDR;

/**
 * 
 * @author dd
 */
@Slf4j
@RequiredArgsConstructor
public abstract class JobWorker extends AbstractVerticle {
  private final String jmPrefix;
  private DeliveryOptions deliveryOptions;

  private JsonObject pickedJob=null;

  @Override
  public void start() throws Exception {
    String myAddr = deploymentID();
    log.info("Ready: " + this + " myAddr=" + myAddr);
    deliveryOptions = JobMarket.createWorkerHeader(myAddr);

    vertx.eventBus().consumer(myAddr, jobListHandler);
  }

  public void register() {
    vertx.eventBus().send(jmPrefix + BUS_ADDR.REGISTER, null, deliveryOptions);
  }

  public void sendJobProgress() {
    checkNotNull(pickedJob, "No job picked!");
    vertx.eventBus().send(jmPrefix + BUS_ADDR.SET_PROGRESS, pickedJob, deliveryOptions);
  }

  public void jobDone() {
    checkNotNull(pickedJob, "No job picked!");
    JsonObject prevJob = pickedJob;
    pickedJob = null; // set to null before notifying jobMarket, which will offer more jobs
    vertx.eventBus().send(jmPrefix + BUS_ADDR.DONE, prevJob, deliveryOptions);
  }

  public void jobFailed() {
    checkNotNull(pickedJob, "No job picked!");
    JsonObject prevJob = pickedJob;
    pickedJob = null; // set to null before notifying jobMarket, which will offer more jobs
    vertx.eventBus().send(jmPrefix + BUS_ADDR.FAIL, prevJob, deliveryOptions);
  }

  @Setter
  private Handler<Message<JsonArray>> jobListHandler = msg -> {
    JsonArray jobs = msg.body();
    checkState(pickedJob==null, "Job in progress! "+pickedJob);
    pickedJob=pickJob(jobs);
    
    msg.reply(pickedJob, deliveryOptions);  // must reply even if picked==null

    if (pickedJob != null) {
      if (doWork(pickedJob))
        jobDone();
      else
        jobFailed();
    }
  };

  protected JsonObject pickJob(JsonArray jobs) {
    log.debug("jobs={}", jobs);
    JsonObject picked=null;
    if (jobs.size() > 0) {
      picked = jobs.getJsonObject(0);
    }
    log.info("pickedJob={} from jobs={}", picked, jobs);
    return picked;
  }

  public abstract boolean doWork(JsonObject job);

}
