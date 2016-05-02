package net.deelam.vertx.jobstore;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.VerticleUtils;

@RunWith(VertxUnitRunner.class)
@Slf4j
public class JobConsignerTest {

  /*  @Test
  public void testJobStore() {
    JobConsigner jc = new JobConsigner();
    TreeSet<JsonObject> set = jc.jobs;
    set.add(new JsonObject().put(ID_ATTRIBUTE, "a"));
    set.add(new JsonObject().put(ID_ATTRIBUTE, "b"));
    set.add(new JsonObject().put(ID_ATTRIBUTE, "1"));
    log.info("set=" + set);
  
    JsonArray jo = new JsonArray();
    set.forEach(e -> jo.add(e));
    log.info("jo=" + jo);
  }*/
  
//  @Rule
//  public RunTestOnContext rule = new RunTestOnContext();

  private Vertx vertx;
  
  JobProducer prod;
  JobConsumer cons;
  
  @Before
  public void before(final TestContext context){
    vertx = Vertx.vertx();
    Async async = context.async(3); //needed for multi-verticle deployments; use context.asyncAsserSuccess() for 1 verticle 
    log.info("Before: "+context);

    Handler<AsyncResult<String>> deployHandler=res->{
      async.countDown();
      log.info("Async.complete " + async.count());
    };

    String jcPrefix = "net.deelam.jc";
    JobConsigner jc = new JobConsigner(jcPrefix);
    prod = jc.createJobProducer();
    cons = jc.createJobConsumer();
    
    vertx.deployVerticle(jc, deployHandler);
    vertx.deployVerticle(prod, deployHandler);
    vertx.deployVerticle(cons, deployHandler);
    async.await(3000); // fails when timeout occurs
  }
  
  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }
  
  @Test
  public void testConsignerInVertx(TestContext context) {
    int numAsserts = 4;
    Async async = context.async(numAsserts);
    
    prod.addJobCompletionHandler(msg -> {
      log.info("!!!!!!!!!! Job complete={}", msg.body());
      log.error("Checking false assertion");
      context.assertTrue(true);
      async.countDown();
    });

    log.info("Testing: ---------------------");
    log.info("Verticles: " + vertx.deploymentIDs());

    JobJO job = new JobJO("a");
    prod.addJob(job);

    JobJO jobB = new JobJO("b");
    prod.addJob(jobB);

    jobB = new JobJO("b");
    cons.startedJob(jobB);
    cons.failedJob(jobB);
    
    JobJO job1 = new JobJO("1");
    prod.addJob(job1, (reply) -> {
    });

    cons.listJobs((reply) -> {
      if (reply.succeeded()) {
        log.info("Jobs={}", reply.result().body());
        async.countDown();
      } else {
        log.info("? reply: " + reply);
      }
    });

    job1 = new JobJO("1");
    job1.put("progress", 50);
    cons.updateJob(job1);
    cons.completedJob(job1);
    
    cons.startedJob(jobB);
    cons.failedJob(jobB);

    cons.listJobs((reply) -> {
      if (reply.succeeded()) {
        log.info("Jobs={}", reply.result().body());
        async.countDown();
      } else {
        log.info("? reply: " + reply);
      }
    });
    
    log.error("Checking false assertion");
    context.assertTrue(true);
    async.countDown();
    
    async.await(3000); // fails when timeout occurs
  }

}
