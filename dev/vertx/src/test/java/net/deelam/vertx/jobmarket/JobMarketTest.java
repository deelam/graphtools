package net.deelam.vertx.jobmarket;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import lombok.extern.slf4j.Slf4j;

@RunWith(VertxUnitRunner.class)
@Slf4j
public class JobMarketTest {

  private Vertx vertx;

  static final String svcType = "typeTestA";

  JobProducer prod;
  JobWorker cons;

  @Before
  public void before(final TestContext context) {
    vertx = Vertx.vertx();
    Async async = context.async(3); //needed for multi-verticle deployments; use context.asyncAsserSuccess() for 1 verticle 
    log.info("Before: " + context);

    Handler<AsyncResult<String>> deployHandler = res -> {
      async.countDown();
      log.info("Async.complete " + async.count());
    };

    JobMarket jm = new JobMarket(svcType);
    prod = new JobProducer(svcType);
    cons = new JobWorker(svcType){
      int count=0;
      public boolean doWork(JsonObject job) {
        boolean success=true; //++count % 2 == 0;
        try {
          job.put("progress", "10%");
          sendJobProgress();
          Thread.sleep(2000);
          job.put("progress2", "50%");
          sendJobProgress();
          prod.getProgress("id-A", reply -> log.info("Progress="+(reply.result()==null ? reply.succeeded() : reply.result().body())));
          if(success){
            Thread.sleep(2000);
            job.put("progress", "100%");
            sendJobProgress();
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return success;
      }
    };

    vertx.deployVerticle(jm, deployHandler);
    vertx.deployVerticle(prod, deployHandler);
    
    DeploymentOptions consumerOpts=new DeploymentOptions().setWorker(true);
    vertx.deployVerticle(cons, consumerOpts, deployHandler);
    
    async.await(30000); // fails when timeout occurs
    log.info("Before done: ---------------------" + vertx.eventBus());
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void test(TestContext context) {
    log.info("Verticles: " + vertx.deploymentIDs() + " " + vertx.eventBus());
    //cons.register();
    
    int numAsserts = 2;
    Async async = context.async(numAsserts);

    prod.addJobCompletionHandler( (Message<JsonObject> msg) -> {
      log.info("==========> Job complete={}", msg.body());
      if(false)
        prod.removeJob(msg.body().getString(JobMarket.JOBID), null);
      //log.info("Checking false assertion ");
      async.countDown();
    });

    JsonObject job = new JsonObject().put("a", "aaaa");
    prod.addJob("id-A", job);

    JsonObject jobB = new JsonObject().put("b", "bbbb");
    prod.addJob("id-B", jobB);
    
    async.await(150000); // fails when timeout occurs
    
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
