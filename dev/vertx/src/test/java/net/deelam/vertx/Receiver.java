package net.deelam.vertx;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;

/*
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Receiver extends AbstractVerticle {

  // Convenience method so you can run it in your IDE
  public static void main(String[] args) throws UnknownHostException {
    InetAddress lh = Inet4Address.getLocalHost();
    System.out.println();
    Runner.runClusteredExample(Receiver.class, "172.17.0.1");
    //Runner.runClusteredExample(EmptyVerticle.class, "172.17.0.1");
  }
  
  public static class EmptyVerticle extends AbstractVerticle{
    
  }

  @Override
  public void start() throws Exception {

    EventBus eb = vertx.eventBus();

    MessageConsumer<Object> c = eb.consumer("ping-address", message -> {

      System.out.println("Received message: " + message.body());
      try {
        Thread.sleep(20);
      } catch (Exception e) {
        e.printStackTrace();
      }
      // Now send back reply
      System.out.println("  Replying to "+message.replyAddress()+": "+ message.body());
      message.reply("To "+message.replyAddress()+" pong! "+ message.body());
    });
    
    c.exceptionHandler(t-> System.err.println("!!!!!!!!!!  "+t));
    
//    eb.addInterceptor(sc->{
//      System.out.println("----- "+sc.message().body());
//      sc.next();
//    });
    
    //vertx.setPeriodic(2000, l -> System.out.println(c.isRegistered()));

    System.out.println("Receiver ready!");
  }
}