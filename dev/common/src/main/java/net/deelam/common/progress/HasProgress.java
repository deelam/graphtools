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
    String message;

    static final Map<String, Number> EMPTY_MAP = Collections.unmodifiableMap(new HashMap<>(0));
    Map<String, Number> metrics = EMPTY_MAP;

    public ProgressState(int percent, String message) {
      this.percent = percent;
      this.message = message;
    }

    public ProgressState setFailed(Throwable e) {
      if(getPercent()==0)
        setPercent(-1);
      else if(getPercent()>0)
        setPercent(-getPercent());
      setMessage(e.getMessage());
      return this;
    }

  }
}
