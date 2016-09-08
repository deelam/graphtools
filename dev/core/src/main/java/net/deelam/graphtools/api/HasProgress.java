package net.deelam.graphtools.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

public interface HasProgress {

  ProgressState getProgress();

  @AllArgsConstructor
  @Data
  @Accessors(fluent = true)
  public static class ProgressState {
    int percent = 0;
    String message;

    static final Map<String, Number> EMPTY_MAP = Collections.unmodifiableMap(new HashMap<>(0));
    Map<String, Number> metrics = EMPTY_MAP;

    public ProgressState(int percent, String message) {
      this.percent = percent;
      this.message = message;
    }

  }
}
