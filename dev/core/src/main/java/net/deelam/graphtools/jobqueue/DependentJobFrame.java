package net.deelam.graphtools.jobqueue;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.frames.Adjacency;
import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.modules.typedgraph.TypeValue;

@Deprecated
@TypeValue(DependentJobFrame.TYPE_VALUE)
public interface DependentJobFrame extends BaseConcept {
  static final String TYPE_VALUE="DependentJob";

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

  @Property(value="jobType")
  String getJobType();
  
  @Property(value="jobType")
  void setJobType(String jobType);
  
  @Property(value="progress")
  int getProgress();

  @Property(value="progress")
  void setProgress(int progress);
  
  public static final String FEEDS_LABEL = "feeds";
  
  @Adjacency(direction=Direction.IN,label=FEEDS_LABEL)
  void addInputJob(DependentJobFrame inJob);
  
  @Adjacency(direction=Direction.OUT,label=FEEDS_LABEL)
  void addOutputJob(DependentJobFrame outJob);
  
  @Adjacency(direction=Direction.IN,label=FEEDS_LABEL)
  Iterable<DependentJobFrame> getInputJobs();
  
  @Adjacency(direction=Direction.OUT,label=FEEDS_LABEL)
  Iterable<DependentJobFrame> getOutputJobs();

  
}
