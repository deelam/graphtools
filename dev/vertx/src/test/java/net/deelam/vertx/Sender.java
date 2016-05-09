package net.deelam.vertx;
import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.Data;

/*
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Sender extends AbstractVerticle {

  static Object myPojo= new Pojo();
  
  @Data
  static class Pojo {
    Date date=new Date();
  }
  
  // Convenience method so you can run it in your IDE
  public static void main(String[] args) throws InterruptedException {
    String jsonStr = Json.encode(myPojo);
    System.out.println(": "+jsonStr);
    if(true){
      Vertx vertx = Vertx.vertx();
      vertx.deployVerticle(Receiver.class.getName());
      Thread.sleep(3000);
      vertx.deployVerticle(Sender.class.getName());
    }else
      Runner.runClusteredExample(Sender.class);
  }

  @Override
  public void start() throws Exception {
    EventBus eb = vertx.eventBus();

    // Send a message every second
    
    AtomicInteger count=new AtomicInteger(0);
    vertx.setPeriodic(10, v -> {
//      String jsonStr =Json.encode(myPojo);
//      JsonObject body = new JsonObject(jsonStr);
      String body=count.incrementAndGet()+" ";
      System.out.println("Sending: "+ body);
      eb.send("ping-address", body, reply -> {
        if(reply.result()!=null)
          System.out.println("Received reply at addr="+reply.result().address()+" "+body);
        if (reply.failed()) {
          System.err.println("  Failed for "+body+"; cause: " + reply.cause());
        }
        if (reply.succeeded()) {
          System.out.println("  Success reply: " + reply.result().body());
        } else {
          System.err.println("  No reply for "+body);
        }
      });

    });
  }
}