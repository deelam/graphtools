package net.deelam.common.progress;

import net.deelam.common.progress.HasProgress.ProgressState;

/**
 * Polls process for status.
 * Receives push updates from process.
 */
public interface ProgressMonitor extends AutoCloseable {
  
	void setProgressMaker(HasProgress process);

	void update(ProgressState p);
	
	void stop();

    void addTargetVertxAddr(String vertxAddrPrefix);
    
    public interface Factory {
      ProgressMonitor create(String jobId, int pollIntervalInSeconds, String busAddr);
    }
}
