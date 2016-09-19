package net.deelam.vertx;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.Json;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Reminder: All bean classes and nested classes of fields must have no-arg constructor (which can be private)
 * @author dnlam
 */
@RequiredArgsConstructor
@Slf4j
public class BeanJsonCodec<C> implements MessageCodec<C,C> {
  
  static Map<Class<?>,EventBus> eventBuses=new HashMap<>();
  public synchronized static <C> void register(EventBus eb, Class<C> beanClass){
    if(eventBuses.containsKey(beanClass))
      log.info("Already registered BeanJsonCodec<{}> with {}; skipping registration", beanClass, eb);
    else {
      try{
        // throws error if already registered
        eb.registerDefaultCodec(beanClass, new BeanJsonCodec<C>(beanClass));
        eventBuses.put(beanClass, eb);
      }catch(Exception e){
        log.error("", e); // log it and keep going
      }
    }
  }
  
  ///
  
  private final Class<C> beanClass;
  
  @SuppressWarnings("unchecked")
  @Setter
  private Function<C,C> beanCopier=(Function<C, C>) DEFAULT_BEANCOPIER;

  private static final Function<?,?> DEFAULT_BEANCOPIER = c -> c;

  @Override
  public void encodeToWire(Buffer buffer, C s) {
    log.info("Encoding {}", s);
    // Encode object to string
    String jsonToStr = Json.encode(s);

    // Length of JSON: is NOT characters count
    int length = jsonToStr.getBytes().length;

    // Write data into given buffer
    buffer.appendInt(length);
    buffer.appendString(jsonToStr);
  }

  @Override
  public C decodeFromWire(int pos, Buffer buffer) {
    // My custom message starting from this *pos* of buffer
    int _pos = pos;

    // Length of JSON
    int length = buffer.getInt(_pos);
    log.info("Decoding buffer of length={}", length);

    // Get JSON string by it`s length
    // Jump 4 because getInt() == 4 bytes
    String jsonStr = buffer.getString(_pos+=4, _pos+=length);
    log.info("Decoding {}", jsonStr);
    C jobDTO = Json.decodeValue(jsonStr, beanClass);

    // We can finally create custom message object
    return jobDTO;
    }

  @Override
  public C transform(C s) {
    return beanCopier.apply(s);
  }

  @Override
  public String name() {
    return this.getClass().getSimpleName()+":"+beanClass.getSimpleName();
  }

  @Override
  public byte systemCodecID() {
    return -1;
  }
  
}