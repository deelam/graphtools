package net.deelam.vertx;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;

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
public class VerticleDiscoveryTest2 {

  private Vertx vertx;

  static HashSet<String> clients=new HashSet<>();

  public static class Server extends AbstractVerticle {
    @Override
    public void start() throws Exception {
      String serverEventBusAddr = deploymentID() + ".addr";
      vertx.eventBus().consumer(serverEventBusAddr, msg -> {
        log.info(serverEventBusAddr+": Discovered client: " + msg.body());
        clients.add((String) msg.body());
      });
      VerticleUtils.announceServiceType(vertx, "typeA", serverEventBusAddr);
    }
  }

  public static class Client extends AbstractVerticle {
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
    Async async = context.async(1); //needed for multi-verticle deployments; use context.asyncAsserSuccess() for 1 verticle 
    log.info("Before: " + context);

    Handler<AsyncResult<String>> deployHandler = res -> {
      async.countDown();
      log.info("deploy " + async.count()+": "+res.result()+" "+res.cause());
    };

    clients.clear();
//    vertx.deployVerticle(Server.class.getName(), deployHandler);
    vertx.deployVerticle(Client.class.getName(), deployHandler);

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
  public void testServerDelayedDeploy(TestContext context) {
    try {
      Thread.sleep(1000);
      vertx.deployVerticle(Server.class.getName());
      Thread.sleep(1000);
      assertEquals(1, clients.size());
      vertx.deployVerticle(Client.class.getName());
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertEquals(2, clients.size());
  }

}
