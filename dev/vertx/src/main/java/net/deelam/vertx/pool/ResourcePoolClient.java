package net.deelam.vertx.pool;

import java.net.URI;
import java.util.function.Consumer;

import org.boon.collections.MultiMap;
import org.boon.collections.MultiMapImpl;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.graphfactories.IdGraphFactoryTinker;
import net.deelam.vertx.VerticleUtils;
import net.deelam.vertx.pool.PondVerticle.ADDR;


@RequiredArgsConstructor
@Slf4j
public class ResourcePoolClient extends AbstractVerticle {
  
  final String pondId;
  String pondAddr;
  
  @Setter
  Consumer<Message<String>> resourceConsumer=null;
  
  @Override
  public void start() throws Exception {
    VerticleUtils.announceClientType(vertx, pondId, msg ->{
      pondAddr=msg.body();
      log.info(pondId+": found pond={}", pondAddr);
    });
    
    vertx.eventBus().consumer(deploymentID(), (Message<String> msg)->{
      log.info(pondId+": Got it! {}", msg.body());
      String origUri=msg.headers().get(PondVerticle.ORIGINAL_URI);
      Consumer<Message<String>> syncToken = null;
      synchronized(waitingObjs){
        syncToken = waitingObjs.getFirst(origUri);
        if(syncToken==null){
          log.error("Cannot find {} in {}", origUri, waitingObjs.baseMap());
        }else{
          if(!waitingObjs.removeValueFrom(origUri, syncToken))
            log.warn("Could not remove {} from {}", syncToken, waitingObjs.baseMap());
          if(waitingObjs.getFirst(origUri)==null){
            log.info("Removing empty key={} from {}", origUri, waitingObjs.baseMap());
            waitingObjs.remove(origUri);
          }
        }
      }

      if(syncToken!=null) synchronized(syncToken){
        syncToken.accept(msg);
        syncToken.notify();
      }
      
      if(resourceConsumer!=null)
        resourceConsumer.accept(msg);
    });
  }

  public void add(URI resourceUri){
    add(resourceUri.toString());
  }
  public void add(String resourceUri){
    vertx.eventBus().send(pondAddr+ADDR.ADD, resourceUri);
  }

  // use synchronized version instead, otherwise this will result in ERROR log "Cannot find {origUri} in {waitingObjs}"
  protected void checkout(String resourceUri){
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

  public void checkin(URI resourceUri){
    checkin(resourceUri.toString());
  }
  
  //
  
  private MultiMap<String,Consumer<Message<String>>> waitingObjs=new MultiMapImpl<>();
  
  public Consumer<Message<String>> checkoutSynchronized(String resourceUri, Consumer<Message<String>> syncToken){
    synchronized(waitingObjs){
      log.info("Putting {} into waitingGraphUris={}", resourceUri, waitingObjs.baseMap());
      waitingObjs.put(resourceUri, syncToken);
    }

    synchronized(syncToken){
      checkout(resourceUri); // async; can finish at any time
      try {
        log.info("Waiting for pool to get resource="+resourceUri);
        syncToken.wait(); // wait for response
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return syncToken;
  }

  public static void main(String[] args) {
    IdGraphFactoryTinker.register();
    MultiMap<String,GraphUri> mm=new MultiMapImpl<>();
    mm.put("a", new GraphUri("tinker:/"));
    mm.put("a", new GraphUri("tinker:///"));
    System.out.println(mm.baseMap());
    GraphUri first = mm.getFirst("a");
    System.out.println(new GraphUri("tinker:/").equals(first)+" "+first);
    mm.removeValueFrom("a", first);
    System.out.println(mm.baseMap());
    
    mm.put("a", new GraphUri("tinker:///asdf"));
    GraphUri next = mm.getFirst("a");
    System.out.println((new GraphUri("tinker:///").equals(next))+" "+next);
    mm.removeValueFrom("a", next);
    System.out.println(mm.baseMap());
    
    GraphUri last = mm.getFirst("a");
    mm.removeValueFrom("a", last);
    System.out.println(mm.baseMap());
    System.out.println(mm.getFirst("a")==null);
    if(mm.getFirst("a")==null)
      mm.remove("a");
    System.out.println(mm.baseMap().size()==0);
  }
  

}