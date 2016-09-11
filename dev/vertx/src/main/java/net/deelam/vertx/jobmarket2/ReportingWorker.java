package net.deelam.vertx.jobmarket2;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.inject.Inject;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import net.deelam.graphtools.api.HasProgress;
import net.deelam.graphtools.api.ProgressMonitor;

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
    try (ProgressMonitor pm = pmProvider.apply(job)) {
      pm.setProgressMaker(this);
      doer.accept(job);
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public ReportingWorker createProgressMonitor(ProgressMonitor.Factory pmFactory) {
    pmProvider = job -> {
      return pmFactory.create(job.getId(), job.getProgressPollInterval(), job.getRequesterAddr());
    };
    return this;
  }

  @Setter
  private Function<JobDTO, ProgressMonitor> pmProvider=DUMMY_PM_PROVIDER;

  private static Function<JobDTO, ProgressMonitor> DUMMY_PM_PROVIDER = job -> {
    return new ProgressMonitor() {

      @Override
      public void close() throws Exception {}

      @Override
      public void setProgressMaker(HasProgress process) {}

      @Override
      public void update(ProgressState p) {}

      @Override
      public void stop() {}

      @Override
      public void addTargetVertxAddr(String vertxAddrPrefix) {}
    };
  };
}
