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

  static final String jcPrefix = "net.deelam.jm-";

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

    JobMarket jm = new JobMarket(jcPrefix);
    prod = new JobProducer(jm.getAddressPrefix());
    cons = new JobWorker(jm.getAddressPrefix()){
      int count=0;
      public boolean doWork(JsonObject job) {
        boolean success=++count % 2 == 0;
        try {
          job.put("progress", "10%");
          sendJobProgress();
          Thread.sleep(2000);
          job.put("progress2", "50%");
          sendJobProgress();
          prod.getProgress("id-A", reply -> log.info("Progress="+reply.result().body()));
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
    
    async.await(3000); // fails when timeout occurs
    log.info("Before done: ---------------------" + vertx.eventBus());
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void test(TestContext context) {
    int numAsserts = 1;
    Async async = context.async(numAsserts);
    async.countDown();

    prod.setJobCompletionHandler(msg -> {
      log.info("!!!!!!!!!! Job complete={}", msg.body());
      log.error("Checking false assertion ");
      context.assertTrue(true);
      async.countDown();
    });

    log.info("Verticles: " + vertx.deploymentIDs() + " " + vertx.eventBus());


    JsonObject job = new JsonObject().put("a", "aaaa");
    prod.addJob("id-A", job);

    JsonObject jobB = new JsonObject().put("b", "bbbb");
    prod.addJob("id-B", jobB);

    cons.register();
/*    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    cons.jobFailed();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    cons.jobDone();
*/
    try {
      Thread.sleep(150000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    //vertx.eventBus().send(jcPrefix + BUS_ADDR.REGISTER, null, JobItem.createConsumerHeader("consumer1"));
    //    vertx.eventBus().send(cons.myAddr, new JsonArray().add(new JsonObject().put("hi", "value")), msg ->{
    //      log.info("got picked="+msg.result().body());
    //    });

  }

  private Handler<AsyncResult<Message<JsonObject>>> defaultAddJobReplyHandler = (reply) -> {
    if (reply.succeeded()) {
      log.info("addJob reply={}", reply.result().body());
    } else if (reply.failed()) {
      log.error("addJob Failed: " + reply.cause());
    } else {
      log.info("addJob unknown reply: " + reply);
    }
  };

}
