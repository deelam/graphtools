package net.deelam.vertx.jobmarket2;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.common.progress.HasProgress.ProgressState;

@RequiredArgsConstructor
@Slf4j
public class SubjobProgressListener extends AbstractVerticle {
  private final String busAddrPrefix;

  private boolean doneAddingSubjobs = false; // needed in case first of many complete before adding all subjobs

  public CompletableFuture<Map<String, AtomicInteger>> doneAddingSubjobs() {
    doneAddingSubjobs = true;
    return CompletableFuture.completedFuture(subjobsProgress);
  }

  private CountDownLatch cdLatch = null;

  public CountDownLatch initNewSubjobs(ProgressState parentJobState, int totalPercent) {
    if (this.state != null)
      throw new IllegalStateException("Previous job not removed: " + this.state);
    this.state = parentJobState;
    log.info("Starting new metajob={}", state.getJobId());
    initialPercent=state.getPercent();
    this.totalPercent=totalPercent;
    subjobsProgress.clear();
    consumer=startListening();
    doneAddingSubjobs = false;
    return cdLatch = new CountDownLatch(1);
  }

  private final Map<String, AtomicInteger> subjobsProgress = new HashMap<>(); // key=requestId
  public void addSubjobRequestId(String requestId) {
    log.debug("Adding subjob: {}", requestId);
    subjobsProgress.put(requestId, new AtomicInteger(0));
  }

  private ProgressState state;
  private int initialPercent, totalPercent;
  private void updateState(String msg) {
    int partialPercent=(computeProgress()*totalPercent)/100;
    state.setPercent(initialPercent+partialPercent).setMessage(msg);
  }

  public int computeProgress() {
    if (subjobsProgress.isEmpty()) {
      //        log.debug("No subjobs to compute progress");
      return 0;
    } else {
      OptionalDouble av = subjobsProgress.entrySet().stream().mapToInt(e -> Math.abs(e.getValue().get())).average();
      if (av.isPresent()) {
        int avProgress = (int) av.getAsDouble();
        log.info("avProgress={} subjobs={}", avProgress, subjobsProgress);
        if (failedCount.get() > 0) {
          return -avProgress;
        } else
          return avProgress;
      } else {
        log.warn("Couldn't compute average: {}", subjobsProgress);
        return 0;
      }
    }
  }

  @Setter
  private boolean quitOnFirstFailure=true;

  private AtomicInteger failedCount = new AtomicInteger(0);
  private AtomicInteger completeCount = new AtomicInteger(0);
  
  private MessageConsumer<JsonObject> startListening() {
    log.info("Start listening to subjobs: ", state.getJobId());
    return vertx.eventBus().consumer(busAddrPrefix, (Message<JsonObject> msg) -> {
      ProgressState subState = Json.decodeValue(msg.body().toString(), ProgressState.class);
      String jobId=subState.getJobId();
      AtomicInteger sjPercent = subjobsProgress.get(jobId);
      if (sjPercent == null)
        log.error("Unknown subjob: {} subjobs={}", jobId, subjobsProgress);
      else if(sjPercent.get()!=subState.getPercent()){
        sjPercent.set(subState.getPercent());
        log.info("Updated {} subjobs={}", jobId, subjobsProgress);

        boolean isParentDone = false;
        if (subState.getPercent() < 0) {
          failedCount.incrementAndGet();
          log.error("{} subjobs failed: {}", failedCount.get(), jobId);
          updateState("Failed "+failedCount.get()+" of "+subjobsProgress.size());
          state.getMetrics().put("FailedSubJobs", failedCount.get());
          if(quitOnFirstFailure)
            isParentDone = true;
        } else if (subState.getPercent() == 100) {
          completeCount.incrementAndGet();
          updateState("Completed subjob "+subState.getJobId());
          state.getMetrics().put("CompletedSubJobs", completeCount.get());
          
          isParentDone = doneAddingSubjobs && subjobsProgress.entrySet().stream().allMatch(e -> {
            int percent = e.getValue().get();
            return percent == 100 || percent < 0;
          });
        }
        if (isParentDone) {
          stopListening();
          state = null;
          cdLatch.countDown();
        }
      } else {
        log.debug("No change");
      }
    });
  }

  private MessageConsumer<JsonObject> consumer = null;

  private void stopListening() {
    log.info("Stop listening to subjobs: {}", subjobsProgress);
    consumer.unregister();
    consumer = null;
  }
}
