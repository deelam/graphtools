package net.deelam.vertx.jobmarket;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.BiFunction;

import javax.inject.Inject;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.api.HasProgress;
import net.deelam.graphtools.api.HasProgress.ProgressState;
import net.deelam.graphtools.api.ProgressMonitor;

/**
 */
@RequiredArgsConstructor
@Slf4j
@ToString
public class VertxProgressMonitor implements ProgressMonitor {

  @RequiredArgsConstructor(onConstructor = @__(@Inject) )
  public static class Factory {
    private final Vertx vertx;

    public VertxProgressMonitor create(String jobId, int pollIntervalInSeconds, String busAddr) {
      return new VertxProgressMonitor(vertx, jobId, pollIntervalInSeconds, busAddr);
    }
  }

  private final Vertx vertx;
  private final String jobId;
  private final int pollIntervalInSeconds;
  private final String busAddr;

  private final JsonObject props = new JsonObject();

  public void setProperty(String key, Object value) {
    props.put(key, value);
  }

  private HasProgress progressMaker = null;
  private Timer timer;

  @Override
  public void setProgressMaker(HasProgress process) {
    if (progressMaker != null)
      throw new IllegalStateException("Cannot set another progressMaker!");

    progressMaker = process;

    if (pollIntervalInSeconds > 0) {
      timer = new Timer();
      timer.schedule(
          new TimerTask() {
            public void run() {
              doUpdate();
            }
          },
          10, //initial delay
          pollIntervalInSeconds * 1000); //subsequent rate
    }
  }

  @Override
  public void close() throws Exception {
    if (progressMaker == null) {
      if (!isStopped) { // then not DONE or FAILED
        log.warn("Assuming progressMaker is done, sending 100%: {}", this);
        update(new ProgressState(100, "Assuming done: " + jobId));
      }
    } else if (!isStopped)
      doUpdate(); // will call stop() if done or failed; otherwise, timer will continue to monitor progressMaker
  }

  boolean isStopped = false;

  @Override
  public void stop() {
    if (isStopped)
      log.warn("stop() already called!");
    isStopped = true;
    if (progressMaker != null) {
      ProgressState p = progressMaker.getProgress();
      if (p.percent() > 0 && p.percent() < 100)
        log.warn("Stopping progress updates for {} before completion: {}", jobId, p);

      if (timer != null)
        timer.cancel();
      progressMaker = null;
      timer = null;
    }
  }

  private void doUpdate() {
    if (progressMaker == null) {
      log.warn("Cannot doUpdate without progressMaker={}", this); // should not occur
      //manually done via update(ProgressState): update(new ProgressState(MIN_PROGRESS, "Activity initialized but has not made progress: " + requestId));
    } else {
      ProgressState p = progressMaker.getProgress();
      log.info("Progress of " + jobId + " by {}: {}", progressMaker, p);

      // accumulate metrics in props
      p.metrics().entrySet().stream().forEach(e -> props.put(e.getKey(), e.getValue()));
      update(p);
    }
  }

  private static final BiFunction<VertxProgressMonitor, ProgressState, JsonObject> DEFAULT_MESSAGE_PROVIDER =
      (pm, state) -> {
        JsonObject jo = new JsonObject(Json.encode(state));
        jo.mergeIn(pm.props);
        
        {
          String statusMsg;
          if (state.percent() >= 100) {
            statusMsg = "Complete: " + pm.jobId + " at "+state.percent();
          } else if (state.percent() < 0) {
            statusMsg = "Failed: " + pm.jobId + " at "+state.percent();
          } else {
            statusMsg = "Progress=" + pm.jobId + " at "+state.percent();
          }
          jo.put("STATUS_MESSAGE", statusMsg);
        }
        return jo;
      };

  @Setter
  private BiFunction<VertxProgressMonitor, ProgressState, JsonObject> messageProvider = DEFAULT_MESSAGE_PROVIDER;

  @Override
  public void update(ProgressState state) {
    { // sanity check
      checkAgainstLastPercent(state);
    }
    
    if (state.percent() < 0 || state.percent() >= 100) {
      stop();
    }
    send(messageProvider.apply(this, state));
  }

  private void send(JsonObject msgObj) {
    vertx.eventBus().publish(busAddr, msgObj);

    if (otherBusAddrs != null) {
      log.debug("Notifying otherBusAddressPrefix={}", otherBusAddrs);
      otherBusAddrs.stream().forEach(addrPrefix -> {
        //log.debug("Notifying "+addrPrefix);
        vertx.eventBus().publish(addrPrefix, msgObj);
      });
    }
  }

  int __sanity_lastPercentSent = 0;
  private void checkAgainstLastPercent(ProgressState state) {
    //log.debug("Sending: {}", progressMsg);
    int percent = state.percent();
    if (percent > 0 && percent < __sanity_lastPercentSent) {
      log.warn("Not expecting to send {} < {}", state.percent(), __sanity_lastPercentSent);
    }
    if (percent > 100)
      log.warn("Not expecting >100: {}", percent);
    __sanity_lastPercentSent = percent;
  }

  private List<String> otherBusAddrs = null;

  @Override
  public void addTargetVertxAddr(String vertxAddrPrefix) {
    if (otherBusAddrs == null) {
      otherBusAddrs = new ArrayList<>();
    }
    otherBusAddrs.add(vertxAddrPrefix);
    log.info("Added to otherBusAddressPrefix={} for {}", otherBusAddrs, jobId);
  }

}
