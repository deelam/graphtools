package net.deelam.graphtools.jobqueue;


public interface DependentJob {
  String getId();
  String getJobType();
  String[] getInputJobs();
}
