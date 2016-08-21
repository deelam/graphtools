package net.deelam.vertx;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Stopwatch;
import com.hazelcast.core.Hazelcast;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.Json;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/*
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@Slf4j
public class Sender extends AbstractVerticle {

  static Object myPojo= new Pojo();
  
  @Data
  static class Pojo {
    Date date=new Date();
  }
  
  static Stopwatch sw=Stopwatch.createStarted();
  
  // Convenience method so you can run it in your IDE
  public static void main(String[] args) throws InterruptedException {
    String jsonStr = Json.encode(myPojo);
    System.out.println(": "+jsonStr);
    if(false){
      Vertx vertx = Vertx.vertx();
      vertx.deployVerticle(Receiver.class.getName());
      Thread.sleep(3000);
      vertx.deployVerticle(Sender.class.getName());
    }else
      Runner.runClusteredExample(Sender.class/*, "172.17.0.2"*/);
  }

  @Override
  public void start() throws Exception {
    System.out.println("Elapsed time: "+sw);// if(true) return;
    EventBus eb = vertx.eventBus();
    
//    eb.addInterceptor(sc->{
//      System.out.println("----- "+sc.message());
//      sc.next();
//    });

    // Send a message every second
    
    AtomicInteger count=new AtomicInteger(0);
    //vertx.setPeriodic(1000, v -> { sendPing(eb, count); });
    //sendPing(eb, count);
    for(int i=1; i<5; ++i){
      vertx.setTimer(1000*i, v -> { sendPing(eb, count); });
    }
    
//    vertx.setTimer(1000*6, v -> { throw new RuntimeException("test"); });
    
    if(!true){
      // This works!!
      vertx.setTimer(1000*7, v -> { 
        System.out.println("Calling vertx.close()");
        log.info("Calling vertx.close()");
        vertx.close(c->{
          System.out.println("Callback to vertx.close() "+c);
          System.err.println("Callback to vertx.close() "+c);
          log.info("Callback to vertx.close() "+c);
        }); 
        try {
          System.out.println("Sleeping");
          Thread.sleep(2000);
          System.out.println("Waking");
        } catch (Exception e) {
          e.printStackTrace();
        }
      }); 
    }else{
      Thread thread = new Thread(()->{
        System.out.println("calling vertx.close()");
        if(false)
          vertx.close(c->{
          // this executes only if I hit Ctrl-C (perhaps because Vertx already has a shutdownHook!)
          log.info("callback to vertx.close() "+c);
          System.out.println("vertx closed");
          //Runner.mgr.leave(r->{});
          Hazelcast.getAllHazelcastInstances().forEach(h->{
            System.err.println("calling h.close() "+h);
            h.getLifecycleService().shutdown();
          });
          System.err.println("calling Hazelcast.shutdownAll() ");
          Hazelcast.shutdownAll();
        });
        
        try {
          int sec=0;
          log.info("Sleeping {} seconds to allow Vertx's shutdown hook to finish completely...", sec);
          Thread.sleep(sec*1000);
          System.out.println("Waking");
        } catch (Exception e) {
          e.printStackTrace();
        }
        
      }, "vertx-shutdown-hook");
      //thread.setDaemon(false);
      Runtime.getRuntime().addShutdownHook(thread);
      
    }
    
    vertx.setTimer(1000*7, v -> { System.exit(0); });
  }

  private void sendPing(EventBus eb, AtomicInteger count) {
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
  }
}