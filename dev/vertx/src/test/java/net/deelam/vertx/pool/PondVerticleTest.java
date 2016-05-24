package net.deelam.vertx.pool;

import java.io.*;
import java.net.URI;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import lombok.extern.slf4j.Slf4j;

@RunWith(VertxUnitRunner.class)
@Slf4j
public class PondVerticleTest {
  private Vertx vertx;

  static final String svcType1 = "dnlam-pcA.pool";
  static final String svcType2 = "dnlam-pcB.pool";

  PondVerticle pond1, pond2;
  ResourcePoolClient client1, client2;


  private PondVerticle.Serializer serializer=(URI origUri, String localPondDir)->{
    File serFile=new File(localPondDir, origUri.getPath());
    try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(serFile))) {
      oos.writeObject("Hey there");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return serFile.toPath();
  };

  private PondVerticle.Deserializer deserializer=(URI origUri, Path localSerializedFile, String localPondDir)->{
    File serFile = new File(localPondDir, origUri.getPath());
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(localSerializedFile.toFile()))) {
      Object obj=ois.readObject();
      try (FileWriter w = new FileWriter(serFile)) {
        w.append(obj.toString());
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return serFile.toURI();
  };

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
    
    client1=new ResourcePoolClient(svcType1);
    client1.setResourceConsumer(msg -> {});
    vertx.deployVerticle(client1, deployHandler);

    client2=new ResourcePoolClient(svcType2);
    client1.setResourceConsumer(msg -> {});
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
    client1.add("string:///hiThere");
    
    client1.checkout("string:///hiThere");
    
    client2.checkout("string:///hiThere");
    
    //Thread.sleep(1000);
  }

}
