/**
 * 
 */
package net.deelam.graphtools.jobqueue;

/**
 * @author dlam
 *
 */
public interface JobProcessor<J extends DependentJob> {

  String getJobType();
  
  boolean precheckJob(J job);

  boolean cancelJob(String jobId);

  boolean isJobReady(J job);

  boolean runJob(J job);

}
