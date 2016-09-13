package net.deelam.vertx.jobmarket2;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.inject.Inject;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.common.progress.HasProgress;
import net.deelam.common.progress.ProgressMonitor;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject) )
public class ReportingWorker implements Function<JobDTO, Boolean>, HasProgress {

  private final Consumer<JobDTO> doer;
  private final Supplier<ProgressState> progressSupplier;

  @Override
  public ProgressState getProgress() {
    return progressSupplier.get();
  }

  @Override
  public Boolean apply(JobDTO job) {
    try (ProgressMonitor pm = progressMonitorProvider.apply(job)) {
      pm.setProgressMaker(this);
      doer.accept(job);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public ReportingWorker setProgressMonitorFactory(ProgressMonitor.Factory pmFactory) {
    if(progressMonitorProvider!=DUMMY_PM_PROVIDER)
      log.warn("Overriding previously set progressMonitorProvider={}", progressMonitorProvider);
    progressMonitorProvider = job -> {
      return pmFactory.create(job.getId(), job.getProgressPollInterval(), job.getRequesterAddr());
    };
    return this;
  }

  @Setter
  private Function<JobDTO, ProgressMonitor> progressMonitorProvider=DUMMY_PM_PROVIDER;

  private static Function<JobDTO, ProgressMonitor> DUMMY_PM_PROVIDER = job -> {
    return new ProgressMonitor() {
      public void close() throws Exception {}
      public void setProgressMaker(HasProgress process) {}
      public void update(ProgressState p) {}
      public void stop() {}
      public void addTargetVertxAddr(String vertxAddrPrefix) {}
    };
  };
}
