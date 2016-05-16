package net.deelam.vertx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import lombok.extern.slf4j.Slf4j;

@RunWith(VertxUnitRunner.class)
@Slf4j
public class VerticleDiscoveryTest {

  private Vertx vertx;

  static final String jcPrefix = "net.deelam.jm-";

  static class Server extends AbstractVerticle {
    @Override
    public void start() throws Exception {
      String serverEventBusAddr = deploymentID() + ".addr";
      String serviceContactInfo = serverEventBusAddr;
      VerticleUtils.announceServiceType(vertx, "typeA", serviceContactInfo);
      vertx.eventBus().consumer(serverEventBusAddr, msg -> {
        log.info("Discovered client: " + msg.body());
      });
    }
  }

  static class Client extends AbstractVerticle {
    @Override
    public void start() throws Exception {
      VerticleUtils.announceClientType(vertx, "typeA", msg->{
        String serviceContactInfo=msg.body();
        vertx.eventBus().send(serviceContactInfo, "Register me="+deploymentID());
      });
    }
  }

  @Before
  public void before(final TestContext context) {
    vertx = Vertx.vertx();
    Async async = context.async(2); //needed for multi-verticle deployments; use context.asyncAsserSuccess() for 1 verticle 
    log.info("Before: " + context);

    Handler<AsyncResult<String>> deployHandler = res -> {
      async.countDown();
      log.info("deploy " + async.count());
    };

    vertx.deployVerticle(new Server(), deployHandler);
    vertx.deployVerticle(new Client(), deployHandler);
    vertx.deployVerticle(new Client(), deployHandler);
//    vertx.deployVerticle(Server.class.getName(), deployHandler);
//    vertx.deployVerticle(Client.class.getName(), deployHandler);

//    DeploymentOptions consumerOpts = new DeploymentOptions().setWorker(true);
//    vertx.deployVerticle(Client.class.getName(), consumerOpts, deployHandler);

    async.await(30000); // fails when timeout occurs
    log.info("Before done: ---------------------" + vertx.eventBus());
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void test(TestContext context) {
    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
