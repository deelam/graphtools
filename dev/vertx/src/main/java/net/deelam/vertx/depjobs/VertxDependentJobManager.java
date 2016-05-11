/**
 * 
 */
package net.deelam.vertx.depjobs;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;
import com.tinkerpop.frames.FramedGraphFactory;
import com.tinkerpop.frames.FramedTransactionalGraph;
import com.tinkerpop.frames.modules.javahandler.JavaHandlerModule;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.depjobs.DependentJobFrame.STATE;
import net.deelam.vertx.jobmarket.JobMarket;
import net.deelam.vertx.jobmarket.JobProducer;

/**
 * 
 * @author dlam
 */
@Slf4j
public class VertxDependentJobManager<T> {

  private final FramedTransactionalGraph<TransactionalGraph> graph;
  
  private final JobProducer jobProd;

  public VertxDependentJobManager(IdGraph<?> dependencyGraph, JobProducer vertxJobProducer) {
    FramedGraphFactory factory = new FramedGraphFactory(new JavaHandlerModule());
    graph = factory.create(dependencyGraph);
    
    jobProd=vertxJobProducer;
    jobProd.addJobCompletionHandler( (Message<JsonObject> msg) -> {
      log.info("==========> Job complete: {}", msg.body());
      String jobId = msg.body().getString(JobMarket.JOBID);
      if(false)
        jobProd.removeJob(jobId, null);
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      log.debug("all jobs: "+this);
      log.info("Done jobId={} \n {}", jobId, toStringRemainingJobs("state", "jobType"));
      jobDone(jobV);
    });
    jobProd.addJobFailureHandler( (Message<JsonObject> msg) -> {
      log.info("==========> Job failed: {}", msg.body());
      String jobId = msg.body().getString(JobMarket.JOBID);
      if(false)
        jobProd.removeJob(jobId, null);
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      log.debug("all jobs: "+this);
      log.info("Failed jobId={} \n {}", jobId, toStringRemainingJobs("state", "jobType"));
      jobFailed(jobV);
    });
  }

  public synchronized void close() {
    synchronized (graph) {
      log.info("Closing {}", this);
      graph.shutdown();
    }
  }

  private Map<String, T> waitingJobs = Collections.synchronizedMap(new HashMap<>());
  private Map<String, T> submittedJobs = Collections.synchronizedMap(new HashMap<>());


  public String toString() {
    return 
        ("Submitted jobs="+submittedJobs.toString())+
        ("\n\tWaiting jobs="+waitingJobs.toString());
    // return GraphUtils.toString(graph, 1000, "jobType", "state");
  }
  
  public String toStringRemainingJobs(String... propsToPrint) {
    StringBuilder sb = new StringBuilder("Nodes:\n");
    int nodeCount = 0;
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
  
  public synchronized void addJob(String jobId, T job, String... inJobIds) {
    // add to graph
    DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
    if (jobV == null) {
      jobV = graph.addVertex(jobId, DependentJobFrame.class);
      //        jobV.setJobType(job.getJobType());
      jobV.setOrder(++counter);
      graph.commit();
    } else {
      throw new IllegalArgumentException("Job with id already exists: " + job);
    }
    addDependentJobs(jobV, inJobIds);

    synchronized (graph) {
      if (isJobReady(jobV)) {
        log.info("Submitting jobId={}", jobV.getNodeId());
        submitJob(jobV, job);
      } else {
        log.info("Input to job={} is not ready; setting state=WAITING.  {}", job);
        putJobInWaitingArea(jobV, job);
      }
    }
  }

  /**
   * If job is waiting, move job to end of queue.
   * If job has been submitted, 
   * @param jobId
   */
  public synchronized void reAddJob(String jobId) {
    // add to graph
    DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
    if (jobV == null) {
      throw new IllegalArgumentException("Job doesn't exist: " + jobId);
    }
    
    T job;
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
    
    synchronized (graph) {
      if (isJobReady(jobV)) {
        log.info("Submitting jobId={}", jobV.getNodeId());
        submitJob(jobV, job);
      } else {
        log.info("Input to job={} is not ready; setting state=WAITING.  {}", job);
        putJobInWaitingArea(jobV, job);
      }
    }
  }
  
  public synchronized void addDependentJobs(String jobId, String... inJobIds) {
    synchronized (graph) {
      DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
      if (jobV == null) {
        throw new IllegalArgumentException("Job with id does not exist: " + jobId);
      } else {
        //log.info("addInputJobs " + jobId);
        addDependentJobs(jobV, inJobIds);
      }
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
    DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
    switch (jobV.getState()) {
      case CANCELLED:
      case FAILED:
      case NEEDS_UPDATE:
      case DONE:
        log.info("Not cancelling. Job's current state={}", jobV.getState());
        break;
      case PROCESSING:
        log.info("Not cancelling. Job's current state={}.  Ask jobProcessor to cancel job.", jobV.getState());
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
    return true;
  }

  void setJobCancelled(DependentJobFrame jobV) {
    synchronized (graph) {
      jobV.setState(STATE.CANCELLED);
      log.info("Cancelled job={}", jobV);
      graph.commit();
    }
  }
  
  private void submitJob(DependentJobFrame jobV, T job) {
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
    JsonObject jobJO=new JsonObject(Json.encode(job));
    jobProd.addJob(jobV.getNodeId(), jobJO);
  }
  
  public STATE getJobStatus(String jobId){
    DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
    checkNotNull(jobV, "Cannot find "+jobId);
    return jobV.getState();
  }
  
  public Map<String, Object> queryJobStats(String jobId) {
    DependentJobFrame jobV = graph.getVertex(jobId, DependentJobFrame.class);
    checkNotNull(jobV, "Cannot find "+jobId);
    
    Map<String,Object> map=new HashMap<>();
    if(jobV.getState() == STATE.PROCESSING){
      jobProd.getProgress(jobId, reply -> {
        synchronized (map) {
          JsonObject bodyJO=(JsonObject) reply.result().body();
          //job = Json.decodeValue(bodyJO.toString(), DependentJob.class);
          map.putAll(bodyJO.getMap());
          map.notify();
        }
      });
      // wait for reply
      synchronized (map) {
        while(map.size()==0) 
          try {
            map.wait();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        return map;
      }
    } else {
      map.put("_jobState", jobV.getState());
      //map.put("_jobProgress", jobV.getProgress());
      return map;
    }
  }

  private void putJobInWaitingArea(DependentJobFrame jobV, T job) {
    synchronized (graph) {
      jobV.setState(STATE.WAITING);
      graph.commit();
    }
    waitingJobs.put(jobV.getNodeId(), job);
    log.info("Waiting jobs: {}", waitingJobs.keySet());
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
        for (DependentJobFrame readyV : readyJobs) { // submit in order
          log.info("Waiting job is now ready; submitting: {}", readyV.getNodeId());
          T job = waitingJobs.remove(readyV.getNodeId());
          submitJob(readyV, job);
        }
        log.info("Waiting jobs: {}", waitingJobs.keySet());
      }
    }
  }

}
