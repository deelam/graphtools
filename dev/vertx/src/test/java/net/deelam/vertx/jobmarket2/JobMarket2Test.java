package net.deelam.vertx.jobmarket2;

import java.util.function.Function;

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
public class JobMarket2Test {

  private Vertx vertx;

  static final String svcType = "typeTestA";

  private static final String jobType = "jobTypeJ";

  JobProducer prod;
  JobWorker cons;

  @Before
  public void before(final TestContext context) {
    vertx = Vertx.vertx();
    Async async = context.async(3); //needed for multi-verticle deployments; use context.asyncAsserSuccess() for 1 verticle 
    log.info("Before: " + context);

    Handler<AsyncResult<String>> deployHandler = res -> {
      if(res.succeeded()){
        async.countDown();
        log.info("Async.complete " + async.count());
      } else {
        log.error("Cause: "+res.cause());
      }
    };

    JobMarket jm = new JobMarket(svcType);
    prod = new JobProducer(svcType);
    cons = new JobWorker(svcType, jobType, new Function<JobDTO, Boolean>() {
      int count=0;
      @Override
      public Boolean apply(JobDTO job) {
        boolean success=true; //++count % 2 == 0;
        try {
          job.getParams().put("progress", "10%");
          cons.sendJobProgress();
          Thread.sleep(2000);
          job.getParams().put("progress2", "50%");
          cons.sendJobProgress();
          prod.getProgress("id-A", reply -> {
            if(reply.succeeded())
              log.info("  Progress="+reply.result().body());
            else
              log.error("  Progress="+reply.cause());
          });
          if(success){
            Thread.sleep(2000);
            job.getParams().put("progress", "100%");
            cons.sendJobProgress();
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return success;
      }
    });

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

    prod.addJobCompletionHandler( (Message<JobDTO> msg) -> {
      log.info("==========> Job complete={}", msg.body());
      if(false)
        prod.removeJob(msg.body().getId(), null);
      //log.info("Checking false assertion ");
      async.countDown();
    });

    JobDTO job = JobDTO.builder().id("id-A").type(jobType).build();
    job.getParams().put("a", "aaaa");
    prod.addJob(job);

    JobDTO jobB = JobDTO.builder().id("id-B").type(jobType).build();
    jobB.getParams().put("b", "bbbb");
    prod.addJob(jobB);
    
    async.await(150000); // fails when timeout occurs
    
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
