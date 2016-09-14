package net.deelam.vertx;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.vertx.core.json.Json;
import net.deelam.common.Pojo;

@JsonTypeInfo( // necessary for polymorphic types
    use = JsonTypeInfo.Id.CLASS,
    include = JsonTypeInfo.As.PROPERTY,
    property = "__class")
public abstract class PojoTypeInfoMixIn {
  public static void register(){
    Json.mapper.addMixIn(Pojo.class, PojoTypeInfoMixIn.class);
  }
}
