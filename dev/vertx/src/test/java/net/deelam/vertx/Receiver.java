package net.deelam.vertx;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;

/*
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class Receiver extends AbstractVerticle {

  // Convenience method so you can run it in your IDE
  public static void main(String[] args) {
    Runner.runClusteredExample(Receiver.class);
  }

  @Override
  public void start() throws Exception {

    EventBus eb = vertx.eventBus();

    eb.consumer("ping-address", message -> {

      System.out.println("Received message: " + message.body());
      try {
        Thread.sleep(20);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      // Now send back reply
      System.out.println("  Replying to "+message.replyAddress()+": "+ message.body());
      message.reply("To "+message.replyAddress()+" pong! "+ message.body());
    });

    System.out.println("Receiver ready!");
  }
}