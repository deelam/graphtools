package net.deelam.vertx.jobmarket2;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferFactoryImpl;
import io.vertx.core.json.Json;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import net.deelam.common.Pojo;
import net.deelam.vertx.BeanJsonCodec;
import net.deelam.vertx.PojoTypeInfoMixIn;

public class JobDTOTest {

  JobDTO obj;
  @Before
  public void setUp() throws Exception {
    obj=new JobDTO("job1", "type1");
    obj.setRequest(new PojoObj());
  }

  @After
  public void tearDown() throws Exception {}

  static{
    PojoTypeInfoMixIn.register();
  }
  
  @Accessors(chain=true)
  @NoArgsConstructor(access=AccessLevel.PRIVATE)
  @Data
  private static class PojoObj implements Pojo<PojoObj>{
    List<String> jobs=new ArrayList<>();
    String str;
    
    @Override
    public PojoObj copy() {
      return new PojoObj().setJobs(jobs).setStr(str);
    }
  }
  
  @Data
  private static class JsonObjectInClass {
    Pojo pojo=new PojoObj();
    String str;
  }
  
  @Test
  public void testJsonObjectInClass() {
    JsonObjectInClass list=new JsonObjectInClass();
    //list.pojo.jobs.add("1");
    list.str="someString";
    
    
    String json = Json.encode(list);
    System.out.println(json);
    JsonObjectInClass decObj = Json.decodeValue(json, JsonObjectInClass.class);
    assertEquals(list, decObj);
  }

  @Test
  public void testJsonCodec() {
    String json = Json.encode(obj);
    System.out.println(json);
    
    JobDTO decObj = Json.decodeValue(json, JobDTO.class);
    assertEquals(obj, decObj);
  }

  @Test
  public void testForEventBus() {
    BeanJsonCodec<JobDTO> codec = new BeanJsonCodec<>(JobDTO.class);
    Buffer buffer=new BufferFactoryImpl().buffer();
    codec.encodeToWire(buffer, obj);
    
    JobDTO decObj = codec.decodeFromWire(0, buffer);
    assertEquals(obj, decObj);
  }

}
