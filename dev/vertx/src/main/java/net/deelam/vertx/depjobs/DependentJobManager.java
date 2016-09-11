/**
 * 
 */
package net.deelam.vertx.depjobs;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.deelam.graphtools.GraphTransaction.begin;
import static net.deelam.graphtools.GraphTransaction.commit;
import static net.deelam.graphtools.GraphTransaction.rollback;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import com.tinkerpop.frames.FramedTransactionalGraph;

import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.FramedGraphProvider;
import net.deelam.graphtools.GraphUtils;
import net.deelam.vertx.depjobs.DependentJobFrame.STATE;
import net.deelam.vertx.jobmarket2.JobDTO;
import net.deelam.vertx.jobmarket2.JobProducer;

/**
 * 
 * @author dlam
 */
@Slf4j
public class DependentJobManager {

  private final FramedTransactionalGraph<TransactionalGraph> graph;
  
  private final JobProducer jobProd;

  public DependentJobManager(IdGraph<?> dependencyGraph, JobProducer vertxJobProducer) {
    Class<?>[] typedClasses = {DependentJobFrame.class};
    FramedGraphProvider provider = new FramedGraphProvider(typedClasses);
    graph = provider.get(dependencyGraph);
    
    jobProd=vertxJobProducer;
    jobProd.addJobCompletionHandler( (Message<JobDTO> msg) -> {
      log.info("==========> Job complete: {}", msg.body());
      String jobId = msg.body().getId();
      if(false)
        jobProd.removeJob(jobId, null);
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      log.debug("all jobs: {}", this);
      log.info("Done jobId={} \n {}", jobId, toStringRemainingJobs(DependentJobFrame.STATE_PROPKEY));
      jobDone(jobV);
    });
    jobProd.addJobFailureHandler( (Message<JobDTO> msg) -> {
      log.info("==========> Job failed: {}", msg.body());
      String jobId = msg.body().getId();
      if(false)
        jobProd.removeJob(jobId, null);
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      log.debug("all jobs: {}", this);
      log.info("Failed jobId={} \n {}", jobId, toStringRemainingJobs(DependentJobFrame.STATE_PROPKEY));
      jobFailed(jobV);
    });
  }

  public synchronized void close() {
    synchronized (graph) {
      log.info("Closing {}", this);
      graph.shutdown();
    }
  }

  private Map<String, JobDTO> waitingJobs = Collections.synchronizedMap(new HashMap<>());
  private Map<String, JobDTO> submittedJobs = Collections.synchronizedMap(new HashMap<>());
  private Map<String, JobDTO> unsubmittedJobs = Collections.synchronizedMap(new HashMap<>());


  public String toString() {
    return 
        ("Submitted jobs="+submittedJobs.toString())+
        ("\n\tWaiting jobs="+waitingJobs.toString());
    // return GraphUtils.toString(graph, 1000, "jobType", "state");
  }
  
  public String toStringRemainingJobs(String... propsToPrint) {
    StringBuilder sb = new StringBuilder("Jobs remaining:\n");
    int nodeCount = 0;
    int tx = begin(graph);
    try {
      //log.info(GraphUtils.toString(graph, 100, "state", "order"));
      for (DependentJobFrame jobV : graph.getVertices(DependentJobFrame.TYPE_KEY, DependentJobFrame.TYPE_VALUE, DependentJobFrame.class)) {
        if (jobV.getState()!=STATE.DONE) {
          ++nodeCount;
          Vertex n = jobV.asVertex();
          sb.append("  ").append(n.getId()).append(": ");
          sb.append(n.getPropertyKeys()).append("\n");
          if (propsToPrint != null && propsToPrint.length > 0) {
            String propValuesStr = toString(n, "\n    ", propsToPrint);
            if (propValuesStr.length() > 0)
              sb.append("    ").append(propValuesStr).append("\n");
          }
        }
      }
      commit(tx);
    } catch (Exception re) {
      rollback(tx);
      throw re;
    }
    sb.append(" (").append(nodeCount).append(" remaining jobs)");
    return (sb.toString());
  }
  private static String toString(Element n, String delim, String... propsToPrint) {
    StringBuilder sb = new StringBuilder();
    if (propsToPrint != null) {
      if (propsToPrint.length == 0) {
        propsToPrint = (String[]) n.getPropertyKeys().toArray(propsToPrint);
      }
      boolean first = true;
      for (String propKey : propsToPrint) {
        if (n.getProperty(propKey) != null) {
          if (first) {
            first = false;
          } else {
            sb.append(delim);
          }
          sb.append(propKey).append("=").append(n.getProperty(propKey).toString());
        }
      }
    }
    return sb.toString();
  }

  public int counter=0;
  
  public synchronized void addJob(JobDTO job, String... inJobIds) {
    addJob(true, job, inJobIds);
  }
  public synchronized void addJob(boolean addToQueue, JobDTO job, String... inJobIds) {
    String jobId=job.getId();
    // add to graph
    int tx = begin(graph);
    try {
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      if (jobV == null) {
        jobV = graph.addVertex(jobId, DependentJobFrame.class);
        //        jobV.setJobType(job.getJobType());
        jobV.setOrder(++counter);
        graph.commit();
      } else {
        throw new IllegalArgumentException("Job with id already exists: " + jobId);
      }
      addDependentJobs(jobV, inJobIds);

      if(addToQueue)
        addToQueue(job, jobV);
      else
        unsubmittedJobs.put(jobId, job);
      
      commit(tx);
    } catch (Exception re) {
      rollback(tx);
      throw re;
    }
  }
  
  private void addToQueue(JobDTO job, DependentJobFrame jobV) {
    synchronized (graph) {
      if (isJobReady(jobV)) {
        log.debug("Submitting jobId={}", jobV.getNodeId());
        submitJob(jobV, job);
      } else {
        log.info("Input to job={} is not ready; setting state=WAITING.", job.getId());
        putJobInWaitingArea(jobV, job);
      }
    }
  }

  public synchronized Collection<String> listJobs(DependentJobFrame.STATE state){
    Collection<String> col=new ArrayList<>();
    int tx = begin(graph);
    try {
      if(state==null){
        for(Vertex v:graph.getVertices()){
          col.add((String) v.getId());
        }
      }else{
        for(Vertex v:graph.getVertices(DependentJobFrame.STATE_PROPKEY, state.name())){
          col.add((String) v.getId());
        }
      }
      commit(tx);
    } catch (Exception re) {
      rollback(tx);
      throw re;
    }
    return col;
  }

  /**
   * If job is waiting, move job to end of queue.
   * If job has been submitted, 
   * @param jobId
   */
  public synchronized void reAddJob(String jobId) {
    int tx = begin(graph);
    try {
      // add to graph
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      if (jobV == null) {
        throw new IllegalArgumentException("Job doesn't exist: " + jobId);
      }
      
      JobDTO job;
      if(jobV.getState()==null)
        job=unsubmittedJobs.get(jobId);
      else
        switch(jobV.getState()){
          case CANCELLED:
          case DONE:
          case FAILED:
          case NEEDS_UPDATE:
            log.info("re-add job: {}", jobId);
            job=submittedJobs.get(jobId);
            break;
          case WAITING:
            jobV.setOrder(++counter);
            log.info("re-add job: {} is currently waiting; adjusting order to {}", jobId, counter);
            return;
          case SUBMITTED:
          case PROCESSING:
          default:
            throw new IllegalStateException("Cannot re-add job "+jobId+" with state="+jobV.getState());
      }
  
      addToQueue(job, jobV);
      
      commit(tx);
    } catch (Exception re) {
      rollback(tx);
      throw re;
    }
  }
  
  public synchronized void addDependentJobs(String jobId, String... inJobIds) {
    synchronized (graph) {
      int tx = begin(graph);
      try {
        DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
        if (jobV == null) {
          throw new IllegalArgumentException("Job with id does not exist: " + jobId);
        } else {
          if(jobV.getState()!=null)
            switch(jobV.getState()){
              case CANCELLED:
              case DONE:
              case FAILED:
              case SUBMITTED:
              case PROCESSING:
                log.warn("Job={} in state={}; adding dependent jobs at this point may be ineffectual: {}", jobV, jobV.getState(), inJobIds);
                break;
              default:
                // okay
            }
          //log.info("addInputJobs " + jobId);
          addDependentJobs(jobV, inJobIds);
        }
        
        commit(tx);
      } catch (Exception re) {
        rollback(tx);
        throw re;
      }
    }
  }

  public boolean hasJob(String jobId){
    int tx = begin(graph);
    try {
      Vertex inputJobV = graph.getVertex(jobId);
      boolean exists = (inputJobV != null);
      commit(tx);
      return exists;
    } catch (Exception re) {
      rollback(tx);
      throw re;
    }
  }
  
  private void addDependentJobs(DependentJobFrame jobV, String... inJobIds) {
    if (inJobIds != null){
      for (String inputJobId : inJobIds) {
        DependentJobFrame inputJobV = graph.getVertex(inputJobId, DependentJobFrame.class);
        if (inputJobV == null)
          throw new IllegalArgumentException("Unknown input jobId=" + inputJobId);
        if(! Iterables.contains(jobV.getInputJobs(), inputJobV))
          jobV.addInputJob(inputJobV);
      }
      graph.commit();
    }
  }

  public synchronized boolean cancelJob(String jobId) {
    int tx = begin(graph);
    try {
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      if(jobV==null)
        throw new IllegalArgumentException("Unknown jobId="+jobId);
      if(jobV.getState()==null){
        unsubmittedJobs.remove(jobId);
        setJobCancelled(jobV);
      } else {
        switch (jobV.getState()) {
          case CANCELLED:
          case FAILED:
          case NEEDS_UPDATE:
          case DONE:
            log.info("Not cancelling {}. Job's current state={}", jobId, jobV.getState());
            break;
          case PROCESSING:
            log.info("Not cancelling {}. Job's current state={}.  Ask jobProcessor to cancel job.", jobId, jobV.getState());
            break;
          case WAITING:
            waitingJobs.remove(jobV.getNodeId());
            setJobCancelled(jobV);
            break;
          case SUBMITTED:
            jobProd.removeJob(jobId, null); // may fail
            submittedJobs.remove(jobId);
            setJobCancelled(jobV);
            break;
        }
      }
      commit(tx);
      return true;
    } catch (Exception re) {
      rollback(tx);
      throw re;
    }
  }

  private void setJobCancelled(DependentJobFrame jobV) {
    synchronized (graph) {
      jobV.setState(STATE.CANCELLED);
      log.info("Cancelled job={}", jobV);
      graph.commit();
    }
  }
  
  public void cancelJobsDependentOn(String jobId, List<String> canceledJobs){
    int tx = begin(graph);
    try {
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      boolean cancelled = cancelJob(jobId);
      if(canceledJobs!=null && cancelled)
        canceledJobs.add(jobId);
      for(DependentJobFrame outJ:jobV.getOutputJobs()){
        cancelJobsDependentOn(outJ.getNodeId(), canceledJobs);
      }
      commit(tx);
    } catch (Exception re) {
      rollback(tx);
      throw re;
    }
  }
  
  private void submitJob(DependentJobFrame jobV, JobDTO job) {
    synchronized (graph) {
      log.debug("submitJob: {}", jobV);
      jobV.setState(STATE.SUBMITTED);
      graph.commit();
    }
    
    //log.debug("----------------------  submitJob: {} {}", job.getClass(), job);
    if(submittedJobs.containsKey(jobV.getNodeId())){
      jobProd.removeJob(jobV.getNodeId(), null);
    }
    submittedJobs.put(jobV.getNodeId(), job);
    jobProd.addJob(job);
  }
  
  public STATE getJobStatus(String jobId){
    int tx = begin(graph);
    try {
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      checkNotNull(jobV, "Cannot find "+jobId);
      commit(tx);
      return jobV.getState();
    } catch (Exception re) {
      rollback(tx);
      throw re;
    }
  }
  
  private ExecutorService threadPool = Executors.newCachedThreadPool();
  
  public Map<String, Object> queryJobStats(String jobId) {
    int tx = begin(graph);
    try {
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      checkNotNull(jobV, "Cannot find "+jobId);
      
      Map<String,Object> map=new HashMap<>();
      if(jobV.getState() == STATE.PROCESSING){
        threadPool.execute(()->{
          jobProd.getProgress(jobId, reply -> {
            synchronized (map) {
              JobDTO bodyJO=reply.result().body();
              //job = Json.decodeValue(bodyJO.toString(), DependentJob.class);
              bodyJO.getParams().forEach(e->map.put(e.getKey(), e.getValue()));
              map.notify();
            }
          });
        });
        
        // wait for reply
        synchronized (map) {
          while(map.size()==0) 
            try {
              map.wait();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          commit(tx);
          return map;
        }
      } else {
        map.put("_jobState", jobV.getState());
        //map.put("_jobProgress", jobV.getProgress());
        commit(tx);
        return map;
      }
    } catch (Exception re) {
      rollback(tx);
      throw re;
    }
  }

  private void putJobInWaitingArea(DependentJobFrame jobV, JobDTO job) {
    synchronized (graph) {
      jobV.setState(STATE.WAITING);
      graph.commit();
    }
    waitingJobs.put(jobV.getNodeId(), job);
    log.info("{} Waiting jobs: {}", waitingJobs.size(), waitingJobs.keySet());
  }
  
  private boolean isJobReady(DependentJobFrame jobV) {
    synchronized (graph) {
      for (DependentJobFrame inV : jobV.getInputJobs()) {
        if (inV.getState() != STATE.DONE)
          return false;
      }
      graph.commit();
    }
    return true;
  }

  private void jobDone(DependentJobFrame job) {
    synchronized (graph) {
      job.setState(STATE.DONE);
      markDependants(job);
      graph.commit();
    }
  }

  private void jobFailed(DependentJobFrame job) {
    synchronized (graph) {
      job.setState(STATE.FAILED);
      graph.commit();
    }
  }
  
  private void markDependants(DependentJobFrame doneJob) {
    /**
     * When a task is DONE, it iterate through each dependee (i.e., task that it feeds):
    If dependee is DONE, change its state to NEEDS_UPDATE if it is not QUEUED.
    If dependee is QUEUED, then don't do anything (it will be processed once popped from queue).
    If dependee NEEDS_UPDATE, do nothing; Client would have to check for task with this state after all submitted relevant requests are complete.
    If dependee is WAITING (was popped earlier), put it in head of queue to be assessed next and if possible processed.
     */
    synchronized(waitingJobs){
      Iterable<DependentJobFrame> outJobs = doneJob.getOutputJobs();
      if (outJobs != null){
        SortedSet<DependentJobFrame> readyJobs=new TreeSet<>( (e1,e2) -> {
          return Integer.compare(e1.getOrder(), e2.getOrder()); });
        for (DependentJobFrame outV : outJobs) {
          if(outV.getState()==null){
            log.info("Not marking job={} as NEEDS_UPDATE since job's current state={} (hasn't been submitted)", outV.getNodeId(), outV.getState());
          } else {
            switch (outV.getState()) {
              case SUBMITTED:
              case DONE:
                log.info("Job dependent on {} has state={}. Marking {} as NEEDS_UPDATE.", doneJob.getNodeId(), outV.getState(), outV.getNodeId());
                outV.setState(STATE.NEEDS_UPDATE);
                break;
              case NEEDS_UPDATE:
                break;
              case WAITING:
                if (isJobReady(outV))
                  readyJobs.add(outV);
                break;
              case PROCESSING:
                log.info("Job dependent on {} is currently processing. Marking {} as NEEDS_UPDATE.", doneJob.getNodeId(), outV.getNodeId());
                outV.setState(STATE.NEEDS_UPDATE);
                break;
              case CANCELLED:
              case FAILED:
                log.info("Not marking job={} as NEEDS_UPDATE since job's current state={}", outV.getNodeId(), outV.getState());
                break;
            }
          }
        }
        for (DependentJobFrame readyV : readyJobs) { // submit in order
          log.info("Waiting job is now ready; submitting: {}", readyV.getNodeId());
          JobDTO job = waitingJobs.remove(readyV.getNodeId());
          submitJob(readyV, job);
        }
        log.debug("Waiting jobs: {}", waitingJobs.keySet());
      }
    }
  }

}
