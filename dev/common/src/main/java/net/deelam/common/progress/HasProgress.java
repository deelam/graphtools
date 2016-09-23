package net.deelam.common.progress;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

public interface HasProgress {

  ProgressState getProgress();

  @NoArgsConstructor
  @AllArgsConstructor
  @Data
  @Accessors(chain = true) // Vertx's eventbus doesn't work with fluents
  public static class ProgressState {
    int percent = 0;
    private long startTime;
    String message;

    static final Map<String, Object> EMPTY_MAP = Collections.unmodifiableMap(new HashMap<>(0));
    Map<String, Object> metrics = EMPTY_MAP;

    public ProgressState(int percent, String message) {
      this.percent = percent;
      this.message = message;
    }

    public void starting(Object object) {
      if (object == null)
        starting(null);
      else
        starting("Starting: " + object.toString());
    }

    public void done(Object object) {
      if (object == null)
        done(null);
      else
        done("Done: " + object.toString());
    }

    public ProgressState starting(String msg) {
      setPercent(1);
      if (msg == null)
        setMessage("Starting");
      else
        setMessage(msg);
      if (metrics == EMPTY_MAP) {
        metrics = new HashMap<>();
      } else if (metrics != null) {
        metrics.clear();
      }
      startTime = System.currentTimeMillis();
      return this;
    }

    public ProgressState done(String msg) {
      setPercent(100);
      if (msg == null)
        setMessage("Done");
      else
        setMessage(msg);
      setElapsedTime();
      return this;
    }

    public ProgressState failed(Throwable e) {
      if (getPercent() == 0)
        setPercent(-1);
      else if (getPercent() > 0)
        setPercent(-getPercent());
      setMessage(e.getMessage());
      setElapsedTime();
      return this;
    }

    private static final String ELAPSED_TIME_SECONDS = "elapsedSeconds";

    private void setElapsedTime() {
      if (metrics != null && metrics != EMPTY_MAP) {
        metrics.put(ELAPSED_TIME_SECONDS, (System.currentTimeMillis() - startTime) / 1000);
      }
    }

  }
}
