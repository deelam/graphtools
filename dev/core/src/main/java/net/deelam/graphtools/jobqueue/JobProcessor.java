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
  
  Class<J> getJobClass();
  
  default boolean precheckJob(J job){ return true; }

  default boolean cancelJob(String jobId){return false;}

  default boolean isJobReady(J job){ return true; }

  boolean runJob(J job);

  boolean runJobJO(DependentJob jobObj);

  
}
