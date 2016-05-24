package net.deelam.vertx.pool;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.graphfactories.IdGraphFactoryNeo4j;
import net.deelam.vertx.VerticleUtils;
import net.deelam.vertx.pool.PondVerticle.ADDR;

@RunWith(VertxUnitRunner.class)
@Slf4j
public class PondVerticleNeo4jGraphTest {
  private Vertx vertx;

  static final String svcType1 = "dnlam-pcA.pool";
  static final String svcType2 = "dnlam-pcB.pool";

  PondVerticle pond1, pond2;
  Client client1, client2;

  private static final String RESOURCE_URI = "neo4j:///./target/hiThere";
  
  @RequiredArgsConstructor
  public static class Client extends AbstractVerticle {
    
    final String svcType;
    String pondAddr;
    
    String resourceUri;
    @Override
    public void start() throws Exception {
      VerticleUtils.announceClientType(vertx, svcType, msg->{
        pondAddr=msg.body();
        log.info(svcType+": found pond={}", pondAddr);
      });
      
      vertx.eventBus().consumer(deploymentID(), msg->{
        resourceUri = (String) msg.body();
        log.info(svcType+": Got it! {}", resourceUri);
      });
    }

    public void add(String resourceUri){
      vertx.eventBus().send(pondAddr+ADDR.ADD, resourceUri);
    }

    public void checkout(String resourceUri){
      JsonObject requestMsg = new JsonObject()
          .put(PondVerticle.CLIENT_ADDR, deploymentID())
          .put(PondVerticle.RESOURCE_URI, resourceUri);

      vertx.eventBus().send(pondAddr+ADDR.CHECKOUT, requestMsg);
    }
    
    public void checkin(String resourceUri){
      JsonObject requestMsg = new JsonObject()
          .put(PondVerticle.CLIENT_ADDR, deploymentID())
          .put(PondVerticle.RESOURCE_URI, resourceUri);

      vertx.eventBus().send(pondAddr+ADDR.CHECKIN, requestMsg);
    }
  }

  @Before
  public void before(final TestContext context) {
    vertx = Vertx.vertx();
    Async async = context.async(4); 
    log.info("Before: " + context);

    Handler<AsyncResult<String>> deployHandler = res -> {
      async.countDown();
      log.info("Async.complete " + async.count());
    };

    pond1 = new PondVerticle(svcType1, 8001);
    vertx.deployVerticle(pond1, deployHandler);
    pond2 = new PondVerticle(svcType2, 8002);
    vertx.deployVerticle(pond2, deployHandler);
    
    PondVerticle.Serializer serializer = SerDeserUtils.createGraphUriSerializer();
    PondVerticle.Deserializer deserializer=SerDeserUtils.createGraphUriDeserializer();
    pond1.register("neo4j", serializer, deserializer);
    pond2.register("neo4j", serializer, deserializer);
    
    client1=new Client(svcType1);
    vertx.deployVerticle(client1, deployHandler);

    client2=new Client(svcType2);
    vertx.deployVerticle(client2, deployHandler);

    async.await(30000); // fails when timeout occurs
    log.info("Before done: ---------------------" + vertx.eventBus());
  }

  @After
  public void tearDown(TestContext context) {
    vertx.close(context.asyncAssertSuccess());
  }

  @Test
  public void test() throws InterruptedException {
    IdGraphFactoryNeo4j.register();
    GraphUri guri=new GraphUri(RESOURCE_URI);
    guri.openIdGraph();
    guri.shutdown();
    
    client1.add(RESOURCE_URI);
    
    client1.checkout(RESOURCE_URI);
    
    
    client2.checkout(RESOURCE_URI);

    Thread.sleep(2000);

    client1.checkin(client1.resourceUri);
    client2.checkin(client2.resourceUri);

    Thread.sleep(3000);
  }

}
