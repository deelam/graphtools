package net.deelam.vertx.pool;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.util.function.BiConsumer;

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
import net.deelam.vertx.VerticleUtils;
import net.deelam.vertx.pool.PondVerticle.ADDR;

@RunWith(VertxUnitRunner.class)
@Slf4j
public class PondVerticleTest {
  private Vertx vertx;

  static final String svcType1 = "dnlam-pcA.pool";
  static final String svcType2 = "dnlam-pcB.pool";

  PondVerticle pond1, pond2;
  Client client1, client2;

  private BiConsumer<URI, File> serializer=(URI uri, File serFile)->{
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(serFile))) {
      oos.writeObject("Hey there");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  };

  private BiConsumer<Path, File> deserializer=(Path serFile, File outFile)->{
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(serFile.toFile()))) {
      Object obj=ois.readObject();
      try (FileWriter w = new FileWriter(outFile)) {
        w.append(obj.toString());
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  };

  @RequiredArgsConstructor
  public static class Client extends AbstractVerticle {
    final String svcType;
    String pondAddr;
    @Override
    public void start() throws Exception {
      VerticleUtils.announceClientType(vertx, svcType, msg->{
        pondAddr=msg.body();
        log.info(svcType+": found pond={}", pondAddr);
      });
      
      vertx.eventBus().consumer(deploymentID(), msg->{
        log.info(svcType+": Got it! {}", msg.body());
      });
    }

    public void add(){
      vertx.eventBus().send(pondAddr+ADDR.ADD, "string:///hiThere");
    }

    public void checkout(){
      JsonObject requestMsg = new JsonObject()
          .put("appAddr", deploymentID())
          .put("resourceUri", "string:///hiThere");

      vertx.eventBus().send(pondAddr+ADDR.CHECKOUT, requestMsg);
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
    
    pond1.register("string", serializer, deserializer);
    pond2.register("string", serializer, deserializer);
    
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
    client1.add();
    
    client1.checkout();
    
    client2.checkout();
    
    Thread.sleep(5000);
  }

}
