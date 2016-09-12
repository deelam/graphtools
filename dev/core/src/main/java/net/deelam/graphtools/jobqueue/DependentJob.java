package net.deelam.graphtools.jobqueue;

@Deprecated
public interface DependentJob {
  String getId();
  String getJobType();
//  String[] getInputJobs();
}
