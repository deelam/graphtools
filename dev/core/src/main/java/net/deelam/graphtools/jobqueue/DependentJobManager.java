/**
 * 
 */
package net.deelam.graphtools.jobqueue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.FramedGraphProvider;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.GraphUtils;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;
import net.deelam.graphtools.jobqueue.DependentJobFrame.STATE;

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
public class DependentJobManager {

  private static final String STOP_JOB_TYPE = "STOP";
  private static final DependentJob STOP_THREAD_JOB = new DependentJobImpl("STOP", STOP_JOB_TYPE, null);

  @AllArgsConstructor
  @Data
  static class DependentJobImpl implements DependentJob {
    String id;
    String jobType;
    String[] inputJobs;
  }

  static class MyProc implements JobProcessor {
    private boolean cancel;

    @Override
    public void runJob(DependentJob job) {
      for (int i = 0; i < 5; ++i) {
        if (cancel) {
          log.info("Cancelling: {}", i);
          //reset
          cancel = false;
          return;
        }
        try {
          log.info(getClass().getSimpleName() + " Running: {}", i);
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    @Override
    public boolean precheckJob() {
      return true;
    }

    @Override
    public boolean cancelJob(String jobId) {
      cancel = true;
      return true;
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    IdGraphFactoryTinker.register();
    GraphUri gUri = new GraphUri("tinker:"/*"neo4j:depJobGraph"*/);
    DependentJobManager mgr = new DependentJobManager(5, gUri.createNewIdGraph(true));

    mgr.addJobProcessor("typeA", new MyProc());
    mgr.addJobProcessor("typeB", new MyProc());

    DependentJob jobA1 = new DependentJobImpl("nodeA1", "typeA", null);
    mgr.addJob(jobA1);
    DependentJob jobA2 = new DependentJobImpl("ALL_SRC", "typeA", new String[] {"nodeA1"});
    mgr.addJob(jobA2);
    DependentJob jobB = new DependentJobImpl("nodeB1", "typeB", new String[] {"ALL_SRC"});
    mgr.addJob(jobB);

    DependentJob jobA3 = new DependentJobImpl("nodeA3", "typeA", new String[] {"nodeA1"});
    mgr.addJob(jobA3);
    DependentJob jobA22 = new DependentJobImpl("ALL_SRC", "typeA", new String[] {"nodeA3"});
    mgr.updateJob(jobA22);

    mgr.addEndJobs();

    Thread.sleep(3000);
    //    mgr.cancelJob(jobA1.getId());

    //    Thread.sleep(1000);
    //    mgr.close();
  }

  private final BlockingDeque<DependentJob> queue = new LinkedBlockingDeque<>();
  private final FramedTransactionalGraph<TransactionalGraph> graph;
  private final List<JobThread> jobRunners;

  public DependentJobManager(int numJobThreads, IdGraph<?> dependencyGraph) {
    Class<?>[] typedClasses = {DependentJobFrame.class};
    FramedGraphProvider provider = new FramedGraphProvider(typedClasses);
    graph = provider.get(dependencyGraph);

    jobRunners = new ArrayList<>(numJobThreads);
    for (int i = 0; i < numJobThreads; ++i) {
      JobThread thread = new JobThread("depJobThread-" + i, this);
      jobRunners.add(thread);
      thread.start();
    }
  }

  public void close() {
    log.info("Closing " + this);
    for (JobThread t : jobRunners) {
      t.setKeepRunning(false);
    }
    if (queue.size() > 0) {
      log.warn("Queue still has {} elements", queue.size());
      queue.clear();
    }
    addEndJobs(); // allows threads to unblock and terminate
    graph.shutdown();
  }

  public void addEndJobs() {
    for (int i = 0; i < jobRunners.size(); ++i) {
      queue.add(STOP_THREAD_JOB);
    }
  }

  private final Map<String, JobProcessor> processors = new HashMap<>();
  private Map<String, DependentJob> waitingJobs = new HashMap<>();

  public void addJobProcessor(String jobType, JobProcessor proc) {
    processors.put(jobType, proc);
  }

  private boolean preCheckJob(DependentJob job) {
    JobProcessor proc = processors.get(job.getJobType());
    if (proc == null) {
      throw new UnsupportedOperationException("type=" + job.getJobType());
    } else {
      if (proc.precheckJob())
        return true;
    }
    return false;
  }

  public void updateJob(DependentJob job) {
    DependentJobFrame jobV = graph.getVertex(job.getId(), DependentJobFrame.class);
    if (jobV == null) {
      throw new IllegalArgumentException("Job with id does not exist: " + job);
    } else {
      log.info("Updating with " + job);
      setInputJobs(job, jobV);
    }
  }

  public boolean addJob(DependentJob job) {
    if (preCheckJob(job)) {
      // add to graph
      DependentJobFrame jobV = graph.getVertex(job.getId(), DependentJobFrame.class);
      if (jobV == null) {
        jobV = graph.addVertex(job.getId(), DependentJobFrame.class);
        jobV.setJobType(job.getJobType());
      } else {
        throw new IllegalArgumentException("Job with id already exists: " + job);
      }
      // TODO: copy job fields to jobV
      setInputJobs(job, jobV);

      // add to queue
      synchronized(graph){ // don't synchronize on queue
        log.info("Adding to queue: " + job);
        queue.add(job);
        jobV.setState(STATE.QUEUED);
      }
      return true;
    } else {
      return false;
    }
  }

  void setInputJobs(DependentJob job, DependentJobFrame jobV) {
    String[] inJobs = job.getInputJobs();
    if (inJobs != null)
      for (String inputJobId : inJobs) {
        DependentJobFrame inputJobV = graph.getVertex(inputJobId, DependentJobFrame.class);
        if (inputJobV == null)
          throw new IllegalArgumentException("Unknown input jobId=" + inputJobId);
        jobV.addInputJob(inputJobV);
      }
  }

  public boolean cancelJob(String jobId) {
    DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
    switch (jobV.getState()) {
      case CANCELLED:
      case FAILED:
      case NEEDS_UPDATE:
      case DONE:
        log.info("Not cancelling. Job's current state={}", jobV.getState());
        break;
      case QUEUED:
        // remove from queue
        synchronized (queue) {
          for (Iterator<DependentJob> itr = queue.iterator(); itr.hasNext(); itr.next()) {
            DependentJob j = itr.next();
            if (j.getId().equals(jobId)) {
              itr.remove();
            }
          }
        }
        setJobCancelled(jobV);
        break;
      case WAITING:
        setJobCancelled(jobV);
        break;
      case PROCESSING:
        // cancel running job
        JobProcessor proc = processors.get(jobV.getJobType());
        log.info("Cancelling job currently run on {}", proc);
        if (proc.cancelJob(jobId)) {
          setJobCancelled(jobV);
        }
        break;
      default:
        throw new IllegalStateException("Unhandled state=" + jobV.getState());
    }
    return true;
  }

  void setJobCancelled(DependentJobFrame jobV) {
    synchronized (graph) {
      jobV.setState(STATE.CANCELLED);
      log.info("Cancelled job={}", jobV);
    }
  }

  public int getProgress(DependentJobFrame job) {
    return job.getProgress();
  }

  public String toString() {
    return GraphUtils.toString(graph, 1000, "state");
  }

  @Slf4j
  static class JobThread extends Thread {
    final DependentJobManager jobMgr;

    public JobThread(String id, DependentJobManager jobMgr) {
      super(id);
      this.jobMgr = jobMgr;
    }

    @Setter
    private boolean keepRunning = true;

    @Override
    public void run() {
      while (keepRunning) {
        //        if(jobMgr.queue.isEmpty()){ // shouldn't happen
        //          try {
        //            wait();
        //            log.info("Woke up!");
        //          } catch (InterruptedException e) {
        //            e.printStackTrace();
        //          }
        //        }else
        try {
          DependentJob job;
          synchronized (jobMgr.queue) {
            job = jobMgr.queue.takeFirst(); // blocks
            log.info(Thread.currentThread().getName() + " takeJob: {} size={}", job, jobMgr.queue.size());
          }
          if (job == null)
            continue;

          if (STOP_JOB_TYPE.equals(job.getJobType())) {
            log.info("Terminating thread: {}", this);
            return;
          }
          DependentJobFrame jobV = jobMgr.graph.getVertex(job.getId(), DependentJobFrame.class);
          if (jobMgr.isJobReady(jobV)) {
            JobProcessor proc = jobMgr.processors.get(job.getJobType());
            synchronized (jobMgr.graph) {
              jobV.setState(STATE.PROCESSING);
            }
            log.info("Starting job={}", job);
            proc.runJob(job);
            jobMgr.jobDone(jobV);
            log.info("Done " + jobV.getNodeId() + " \n" + jobMgr.toString());
          } else {
            synchronized (jobMgr.graph) {
              log.warn("Job is not ready for processing: {}  Setting state=WAITING", job);
              jobV.setState(STATE.WAITING);
              jobMgr.waitingJobs.put(job.getId(), job);
            }
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public boolean isJobReady(DependentJobFrame jobV) {
    synchronized (graph) {
      for (DependentJobFrame inV : jobV.getInputJobs()) {
        if (inV.getState() != STATE.DONE)
          return false;
      }
    }
    return true;
  }

  public void jobDone(DependentJobFrame job) {
    synchronized (graph) {
      job.setState(STATE.DONE);
      try {
        markDependants(job);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private void markDependants(DependentJobFrame doneJob) throws InterruptedException {
    /**
     * When a task is DONE, it iterate through each dependee (i.e., task that it feeds):
    If dependee is DONE, change its state to NEEDS_UPDATE if it is not QUEUED.
    If dependee is QUEUED, then don't do anything (it will be processed once popped from queue).
    If dependee NEEDS_UPDATE, do nothing; Client would have to check for task with this state after all submitted relevant requests are complete.
    If dependee is WAITING (was popped earlier), put it in head of queue to be assessed next and if possible processed.
     */
    Iterable<DependentJobFrame> outJobs = doneJob.getOutputJobs();
    if (outJobs != null)
      for (DependentJobFrame outV : outJobs) {
        switch (outV.getState()) {
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
            log.info("Not marking job={} as NEEDS_UPDATE since job's current state={}", outV.getNodeId(), outV
                .getState());
            break;
        }
      }
  }

  private void pushAheadInQueue(DependentJobFrame outV) throws InterruptedException {
    synchronized (queue) {
      outV.setState(STATE.QUEUED);
      log.info("pushAheadInQueue: {}", outV);
      queue.putFirst(waitingJobs.remove(outV.getNodeId()));
    }
  }

}
