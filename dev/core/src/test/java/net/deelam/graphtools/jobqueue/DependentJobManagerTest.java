package net.deelam.graphtools.jobqueue;

import java.io.IOException;

import org.junit.Test;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;
import net.deelam.graphtools.jobqueue.DependentJobManager.DependentJobImpl;

public class DependentJobManagerTest {

  //@Test
  public void test() throws IOException, InterruptedException {
    IdGraphFactoryTinker.register();
    GraphUri gUri = new GraphUri("tinker:"/*"neo4j:depJobGraph"*/);
    DependentJobManager mgr = new DependentJobManager(gUri.createNewIdGraph(true));
    mgr.startJobRunnerThreads(5);
    
    mgr.addJobProcessor("typeA", new MyProc());
    mgr.addJobProcessor("typeB", new MyProc());

    DependentJob jobA1 = new DependentJobImpl("nodeA1", "typeA");
    mgr.addJob(jobA1);
    DependentJob jobA2 = new DependentJobImpl("ALL_SRC", "typeA");
    mgr.addJob(jobA2, "nodeA1");
    DependentJob jobB = new DependentJobImpl("nodeB1", "typeB");
    mgr.addJob(jobB, "ALL_SRC");

    DependentJob jobA3 = new DependentJobImpl("nodeA3", "typeA");
    mgr.addJob(jobA3, "nodeA1");
    //DependentJob jobA22 = new DependentJobImpl("ALL_SRC", "typeA", new String[] {"nodeA3"});
    mgr.addInputJobs("ALL_SRC", "nodeA3");

    mgr.addEndJobs();

    Thread.sleep(3000);
    //    mgr.cancelJob(jobA1.getId());

    //    Thread.sleep(1000);
    //    mgr.close();
  }

  @Slf4j
  static class MyProc implements JobProcessor<DependentJob> {
    private boolean cancel;

    @Override
    public boolean runJob(DependentJob job) {
      for (int i = 0; i < 5; ++i) {
        if (cancel) {
          log.info("Cancelling: {}", i);
          //reset
          cancel = false;
        }
        try {
          log.info(getClass().getSimpleName() + " Running: {}", i);
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      return true;
    }

    @Override
    public boolean precheckJob(DependentJob job) {
      return true;
    }

    @Override
    public boolean cancelJob(String jobId) {
      cancel = true;
      return true;
    }

    @Override
    public boolean isJobReady(DependentJob job) {
      return true;
    }

    @Override
    public String getJobType() {
      return null;
    }

    @Override
    public Class<DependentJob> getJobClass() {
      return null;
    }

    @Override
    public boolean runJobJO(DependentJob jobObj) {
      return false;
    }
  }


}
