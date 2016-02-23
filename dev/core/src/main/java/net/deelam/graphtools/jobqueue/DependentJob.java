package net.deelam.graphtools.jobqueue;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.frames.Adjacency;
import com.tinkerpop.frames.Property;

public interface DependentJob extends BaseConcept {


  enum STATE {
    QUEUED, // in the queue waiting to be processed
    PROCESSING, // actively in progress
    WAITING, // in WAITING_AREA; has been popped from queue but state was not valid to start processing
    DONE, // completed processing; can transition back to QUEUED if another relevant request is submitted by Client
    CANCELLED,
    FAILED,
    NEEDS_UPDATE // the task was DONE but an input to the task has changed; can transition back to QUEUED if another relevant request is submitted by Client
  }
  
  @Property(value="state")
  STATE getState();
  
  @Property(value="state")
  void setState(STATE state);

  @Property(value="progress")
  int getProgress();

  @Property(value="progress")
  void setProgress(int progress);
  
  public static final String FEEDS_LABEL = "feeds";
  
  @Adjacency(direction=Direction.IN,label=FEEDS_LABEL)
  void addInputJob(DependentJob inJob);
  
  @Adjacency(direction=Direction.OUT,label=FEEDS_LABEL)
  void addOutputJob(DependentJob outJob);
  
  @Adjacency(direction=Direction.IN,label=FEEDS_LABEL)
  Iterable<DependentJob> getInputJobs();
  
  @Adjacency(direction=Direction.OUT,label=FEEDS_LABEL)
  Iterable<DependentJob> getOutputJobs();
  
  
}
