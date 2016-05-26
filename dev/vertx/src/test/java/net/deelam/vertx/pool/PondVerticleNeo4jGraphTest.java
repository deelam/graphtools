package net.deelam.vertx.pool;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
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
  
  public static class Client extends ResourcePoolClient {
    String resourceUri;
    
    public Client(String pondId) {
      super(pondId);
      setResourceConsumer(msg->{
        String uri=msg.body();
        resourceUri = uri;
        GraphUri guri = new GraphUri(uri);
        guri.openIdGraph();
        guri.shutdown();
      });
    }
    
    public void checkin(String resourceUri){
      if(this.resourceUri==null)
        log.error("Didn't get resource yet!");
      super.checkin(resourceUri);
      this.resourceUri=null;
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

    pond1 = new PondVerticle(svcType1);
    vertx.deployVerticle(pond1, deployHandler);
    pond2 = new PondVerticle(svcType2);
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
  public void testVersions(TestContext context) throws InterruptedException {
    IdGraphFactoryNeo4j.register();
    GraphUri guri=new GraphUri(RESOURCE_URI);
    guri.openIdGraph();
    guri.shutdown();
    
    String RESOURCE_URI2=RESOURCE_URI+"?"+PondVerticle.VERSION_PARAM+"=2";
    GraphUri guri2=new GraphUri(RESOURCE_URI2);
    guri2.openIdGraph();
    guri2.shutdown();
    //Thread.sleep(2000);
    
    client1.add(RESOURCE_URI);
    
    client1.checkout(RESOURCE_URI);

    client1.add(RESOURCE_URI2);
    client2.checkout(RESOURCE_URI2);
    
    
    client2.checkout(RESOURCE_URI);

    Thread.sleep(2000);

    client1.checkin(client1.resourceUri);
    client2.checkin(client2.resourceUri);
  }

  @Test
  public void testVersionOverride(TestContext context) throws InterruptedException {
    IdGraphFactoryNeo4j.register();
    GraphUri guri=new GraphUri(RESOURCE_URI);
    guri.openIdGraph();
    guri.shutdown();
    
    client1.add(RESOURCE_URI);
    client1.checkout(RESOURCE_URI);
    client2.checkout(RESOURCE_URI);
    Thread.sleep(1000);
    //client1.checkin(client1.resourceUri);

    client1.add(RESOURCE_URI);
    client1.checkout(RESOURCE_URI);
    Thread.sleep(1000);
    client1.checkin(client1.resourceUri);
  }
}
