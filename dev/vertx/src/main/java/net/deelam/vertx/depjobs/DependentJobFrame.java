package net.deelam.vertx.depjobs;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.frames.Adjacency;
import com.tinkerpop.frames.Property;
import com.tinkerpop.frames.VertexFrame;
import com.tinkerpop.frames.modules.javahandler.JavaHandler;
import com.tinkerpop.frames.modules.typedgraph.TypeField;
import com.tinkerpop.frames.modules.typedgraph.TypeValue;

@TypeField(DependentJobFrame.TYPE_KEY)
@TypeValue(DependentJobFrame.TYPE_VALUE)
public interface DependentJobFrame extends VertexFrame {
  static final String TYPE_KEY="_FRAME_TYPE";
  static final String TYPE_VALUE="DependentJob";

  enum STATE {
    //QUEUED, // in the queue waiting to be processed
    SUBMITTED, // submitted for processing (not necessarily started)
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
  
  @Property(value="order")
  int getOrder();

  @Property(value="order")
  void setOrder(int order);
  
  public static final String FEEDS_LABEL = "feeds";
  
  @Adjacency(direction=Direction.IN,label=FEEDS_LABEL)
  void addInputJob(DependentJobFrame inJob);
  
  @Adjacency(direction=Direction.OUT,label=FEEDS_LABEL)
  void addOutputJob(DependentJobFrame outJob);
  
  @Adjacency(direction=Direction.IN,label=FEEDS_LABEL)
  Iterable<DependentJobFrame> getInputJobs();
  
  @Adjacency(direction=Direction.OUT,label=FEEDS_LABEL)
  Iterable<DependentJobFrame> getOutputJobs();

///
  
  @JavaHandler
  String getNodeId();
  
  //--------- Operations --------------

  abstract class Impl implements DependentJobFrame {
      public static String getFrameType(Vertex v){
          String type=v.getProperty(TYPE_KEY);
          return type;
      }
      public String getNodeId(){
        return asVertex().getId().toString();
    }
  }

}
