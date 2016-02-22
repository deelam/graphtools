/**
 * 
 */
package net.deelam.graphtools.jobqueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.jobqueue.DependentJob.STATE;

import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import com.tinkerpop.frames.FramedTransactionalGraph;

/**
 * TODO: add GraphTransactions
 * 
 * @author dlam
 */
@RequiredArgsConstructor
@Slf4j
public class DependantJobManager {
  private final BlockingDeque<DependentJob> queue=new LinkedBlockingDeque<>();
  private final FramedTransactionalGraph<TransactionalGraph> graph;
  private final List<JobThread> jobRunners;
  
  public DependantJobManager(int numJobThreads, IdGraph<?> dependencyGraph) {
    Class<?>[] typedClasses={ DependentJob.class };
    FramedGraphProvider provider=new FramedGraphProvider(typedClasses);
    graph = provider.get(dependencyGraph);
    
    jobRunners=new ArrayList<>(numJobThreads);
    for(int i=0; i<numJobThreads; ++i){
      JobThread thread = new JobThread(this);
      jobRunners.add(thread);
    }
  }
  
  public void close(){
    for(JobThread t:jobRunners){
      t.setKeepRunning(false);
    }
    if(queue.size()>0){
      log.warn("Queue still has {} elements", queue.size());
      queue.clear();
    }
    // allows threads to unblock and terminate
    for(int i=0;i<jobRunners.size();++i){
      queue.addFirst(null);
    }
    graph.shutdown();
  }
  
  private final Map<String,JobProcessor> processors=new HashMap<>();
  public void addJobProcessor(String jobType, JobProcessor proc){
    processors.put(jobType, proc);
  }
  
  private boolean preCheckJob(DependentJob job){
    JobProcessor proc = processors.get(job.getVertexType());
    if(proc==null){
      throw new UnsupportedOperationException("type="+job.getVertexType());
    }else{
      if(proc.precheckJob())
        return true;
    }
    return false;
  }
  
  public boolean addJob(DependentJob job, String... inputJobIds){
    if(preCheckJob(job)){
      // add to graph
      DependentJob jobV = graph.getVertex(job.getNodeId(), DependentJob.class);
      if(jobV==null){
        jobV = graph.addVertex(job.getNodeId(), DependentJob.class);
      }
      // TODO: copy job fields to jobV
      for(String inputJobId:inputJobIds){
        DependentJob inputJobV = graph.getVertex(inputJobId, DependentJob.class);
        if(inputJobV==null)
          throw new IllegalArgumentException("Unknown input jobId="+inputJobId);
        jobV.addInputJob(inputJobV);
      }
      
      // add to queue
      synchronized(queue){
        log.info("Adding to queue: "+job);
        queue.addLast(job);
        jobV.setState(STATE.QUEUED);
      }
      // wake up all jobthreads if they are idle 
//      for(JobThread threads:jobRunners){
//        threads.notify();
//      }
      return true;
    } else {
      return false;
    }
  }
  
  public boolean cancelJob(String jobId){
    DependentJob jobV = graph.getVertex(jobId, DependentJob.class);
    switch(jobV.getState()){
      case CANCELLED:
      case FAILED:
      case NEEDS_UPDATE:
      case DONE:
        log.info("Not cancelling. Job's current state={}",jobV.getState());
        break;
      case QUEUED:
        // remove from queue
        for(Iterator<DependentJob> itr=queue.iterator(); itr.hasNext(); itr.next()){
          DependentJob j = itr.next();
          if(j.getNodeId().equals(jobId)){
            itr.remove();
          }
        }
        jobV.setState(STATE.CANCELLED);
        break;
      case WAITING:
        jobV.setState(STATE.CANCELLED);
        break;
      case PROCESSING:
        // cancel running job
        JobProcessor proc = processors.get(jobV.getVertexType());
        log.info("Cancelling job currently run on {}", proc);
        if(proc.cancelJob(jobId)){
          jobV.setState(STATE.CANCELLED);
          log.info("Cancelled job={}",jobId);
        }
        break;
      default:
        throw new IllegalStateException("Unhandled state="+jobV.getState());
    }
    return true;
  }
  
  private void markDependants(DependentJob doneJob) throws InterruptedException{
    /**
     * When a task is DONE, it iterate through each dependee (i.e., task that it feeds):
  If dependee is DONE, change its state to NEEDS_UPDATE if it is not QUEUED.
  If dependee is QUEUED, then don't do anything (it will be processed once popped from queue).
  If dependee NEEDS_UPDATE, do nothing; Client would have to check for task with this state after all submitted relevant requests are complete.
  If dependee is WAITING (was popped earlier), put it in head of queue to be assessed next and if possible processed.
     */
    for(DependentJob outV:doneJob.getOutputJobs()){
      switch(outV.getState()){
        case DONE:
          outV.setState(STATE.NEEDS_UPDATE);
          break;
        case QUEUED:
          break;
        case NEEDS_UPDATE:
          break;
        case WAITING:
          pushAheadInQueue(outV);
          break;
        case PROCESSING:
          log.info("Job dependent on {} is currently processing. Marking it as NEEDS_UPDATE.", doneJob.getNodeId());
          outV.setState(STATE.NEEDS_UPDATE);
          break;
        case CANCELLED:
        case FAILED:
          log.info("Not marking job={} as NEEDS_UPDATE since job's current state={}", outV.getNodeId(), outV.getState());
          break;
      }
    }
  }
  
  private void pushAheadInQueue(DependentJob outV) throws InterruptedException {
    synchronized(queue){
      queue.putFirst(outV);
    }
    outV.setState(STATE.QUEUED);
  }

  public int getProgress(DependentJob job){
    return job.getProgress();
  }
  
  @RequiredArgsConstructor
  @Slf4j
  static class JobThread extends Thread {
    final DependantJobManager jobMgr;
    
    @Setter
    private boolean keepRunning=true;
    
    @Override
    public void run() {
      while(keepRunning){
//        if(jobMgr.queue.isEmpty()){ // shouldn't happen
//          try {
//            wait();
//            log.info("Woke up!");
//          } catch (InterruptedException e) {
//            e.printStackTrace();
//          }
//        }else
        try{
          DependentJob job;
          synchronized(jobMgr.queue){
            job = jobMgr.queue.takeFirst(); // blocks
          }
          if(job==null)
            continue;
          if(jobMgr.isJobReady(job)){
            JobProcessor proc = jobMgr.processors.get(job.getVertexType());
            job.setState(STATE.PROCESSING);
            proc.runJob(job);
            jobMgr.jobDone(job);
          }else{
            log.warn("Job is not ready for processing: {}  Setting state=WAITING", job);
            job.setState(STATE.WAITING);
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public boolean isJobReady(DependentJob job) {
    DependentJob jobV=graph.getVertex(job.getNodeId(), DependentJob.class);
    for(DependentJob inV:jobV.getInputJobs()){
      if(inV.getState()!=STATE.DONE)
        return false;
    }
    return true;
  }

  public void jobDone(DependentJob job) {
    job.setState(STATE.DONE);
    try {
      markDependants(job);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }  
}
