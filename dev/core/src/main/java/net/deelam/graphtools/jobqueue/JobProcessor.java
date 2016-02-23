/**
 * 
 */
package net.deelam.graphtools.jobqueue;

/**
 * @author dlam
 *
 */
public interface JobProcessor {

  boolean precheckJob();

  void runJob(DependentJob job);

  boolean cancelJob(String jobId);

}
