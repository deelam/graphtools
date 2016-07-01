package net.deelam.vertx;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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
        clients.add((String) msg.body());
        log.info("server received "+ clients.size() +" " +serverEventBusAddr+": Discovered client: " + msg.body());
      });
      VerticleUtils.announceServiceType(vertx, "typeA", serverEventBusAddr);
    }
  }

  
  static List<String> clientDeployId=new ArrayList<>();
  static int clientCounter=0;
  static int responseMsgCounter=0;
  public static class Client extends AbstractVerticle {
    String name;
    {
      name=++clientCounter+"_client";
      log.debug("Client created {}", clientCounter);
    }
    String serviceContactInfo;
    @Override
    public void start() throws Exception {
      clientDeployId.add(deploymentID());
      VerticleUtils.announceClientType(vertx, "typeA", msg->{
        if(serviceContactInfo==null){
          serviceContactInfo=msg.body();
          ++responseMsgCounter;
          log.debug(name+" client send: {}/{}", responseMsgCounter, clientCounter);
          vertx.eventBus().send(serviceContactInfo, "Register me="+deploymentID()+" "+name);
        }else{
          log.warn(name+" Already got it; ignoring: {}"+msg.body());
        }
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
      Thread.sleep(500);
      vertx.deployVerticle(Server.class.getName());
      Thread.sleep(500);
      assertEquals(1, clients.size());
      vertx.deployVerticle(Client.class.getName());
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertEquals(2, clients.size());
  }

//  @Test
  public void testManyClients(TestContext context) throws InterruptedException {
    vertx.deployVerticle(Server.class.getName());

    for(int j=1; j<150; ++j){
      int numClients = 50;
      for(int i=0; i<numClients; ++i){
        vertx.deployVerticle(Client.class.getName());
      }
      
      Thread.sleep(1000);
      log.debug(j+" Sleep: {}/{}", clientCounter, responseMsgCounter);
      assertEquals(clientCounter, clients.size());
      assertEquals(clientCounter, responseMsgCounter);
      assertEquals(numClients*j+1, clients.size());
      for(String id:clientDeployId){
        vertx.undeploy(id);
      }
      Thread.sleep(1000);
    }

  }

    
}
