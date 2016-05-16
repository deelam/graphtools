package net.deelam.vertx.depjobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.jobmarket.JobMarket;
import net.deelam.vertx.jobmarket.JobProducer;
import net.deelam.vertx.jobmarket.JobWorker;

//@RunWith(VertxUnitRunner.class)
@Slf4j
public class DependentJobManagerTest {

  private Vertx vertx;

  static final String svcType = "DependentJobManagerTest";

  JobProducer prod;
  List<JobWorker> cons=new ArrayList<>();

  static class MyWorker extends JobWorker{
    static int counter=0;
    final int name=++counter;
    public MyWorker() {
      super(svcType);
    }
      public boolean doWork(JsonObject job) {
        for (int i = 0; i < 5; ++i) {
          try {
            log.info(name + " Running: " + job + " {}", i);
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        return true;
      }
  }
  @Before
  public void before() throws InterruptedException {
    vertx = Vertx.vertx();
    JobMarket jm = new JobMarket(svcType);
    prod = new JobProducer(svcType);
    cons.add(new MyWorker());
    cons.add(new MyWorker());

    AtomicInteger async = new AtomicInteger(2+cons.size()); //needed for multi-verticle deployments; use context.asyncAsserSuccess() for 1 verticle 

    Handler<AsyncResult<String>> deployHandler = res -> {
      if(async.decrementAndGet()==0){
        log.info("Async.complete " + async);
        synchronized (DependentJobManagerTest.this) {
          DependentJobManagerTest.this.notify();
        }
      }
    };

    vertx.deployVerticle(jm, deployHandler);
    vertx.deployVerticle(prod, deployHandler);
    
    DeploymentOptions consumerOpts=new DeploymentOptions().setWorker(true);
    cons.forEach(c -> vertx.deployVerticle(c, consumerOpts, deployHandler));
    
    synchronized (this) {
      wait();
    }
    log.info("Before done: ---------------------" + vertx.eventBus());
    
    mgr = new VertxDependentJobManager(new IdGraph(new TinkerGraph()), prod);
  }

  @After
  public void tearDown() {
    vertx.close();
  }

  VertxDependentJobManager mgr;
  
  @AllArgsConstructor
  @Data
  static class DependentJobImpl {
    String id;
    String jobType;
  }

  @Test
  public void test() throws IOException, InterruptedException {
    log.info("Verticles: " + vertx.deploymentIDs() + " " + vertx.eventBus());
    //cons.forEach(c->c.register());
    
//    prod.addJobCompletionHandler( (Message<JsonObject> msg) -> {
//      log.info("==========> Job complete={}", msg.body());
//      if(false)
//        prod.removeJob(msg.body().getString(JobMarket.JOBID));
//      //log.info("Checking false assertion ");
//      async.countDown();
//    });
    
    
    DependentJobImpl jobA1 = new DependentJobImpl("nodeA1", "typeA");
    mgr.addJob("nodeA1", jobA1);
    DependentJobImpl jobB2 = new DependentJobImpl("nodeB2", "typeB");
    mgr.addJob("nodeB2", jobB2);
    DependentJobImpl jobBeta2 = new DependentJobImpl("nodeBeta2", "typeB");
    mgr.addJob("nodeBeta2", jobBeta2);
    DependentJobImpl jobAllSrc = new DependentJobImpl("ALL_SRC", "typeA");
    mgr.addJob("ALL_SRC", jobAllSrc, "nodeA1", "nodeB2", "nodeBeta2");
    DependentJobImpl jobC = new DependentJobImpl("nodeC4", "typeC");
    mgr.addJob("nodeC4", jobC, "ALL_SRC");

    DependentJobImpl jobA3 = new DependentJobImpl("nodeA3a", "typeA");
    mgr.addJob("nodeA3a", jobA3, "nodeA1");
    DependentJobImpl jobA3b = new DependentJobImpl("nodeA3b", "typeA");
    mgr.addJob("nodeA3b", jobA3b, "nodeA1");
    //DependentJob jobA22 = new DependentJobImpl("ALL_SRC", "typeA", new String[] {"nodeA3"});
    mgr.addDependentJobs("ALL_SRC", "nodeA3a", "nodeA3b");
    
    mgr.reAddJob("ALL_SRC");

    //TODO: idle threads die if jobs are blocked: mgr.addEndJobThreads();
//    async.await(150000); // fails when timeout occurs

    Thread.sleep(30000);
    System.out.println(mgr);
    //    mgr.cancelJob(jobA1.getId());

    //    Thread.sleep(1000);
    //    mgr.close();
  }

}
