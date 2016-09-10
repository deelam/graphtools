package net.deelam.vertx.jobmarket2;

import static com.google.common.base.Preconditions.checkNotNull;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class JobDTO {
  String id;
  String type;
  String inputPath, outputPath;
  
  String requesterAddr; // Vertx eventbus address; job worker can register itself to this address

  String paramsClassname;
  JsonObject params; // job-specific parameters

  public JobDTO copy() {
    JobDTO dto = builder().id(id).type(type)
        .inputPath(inputPath).outputPath(outputPath)
        .requesterAddr(requesterAddr)
        .paramsClassname(paramsClassname)
        .build();

    if (params != null)
      dto.params = params.copy();

    return dto;
  }
  
  public JsonObject getParams() {
    if (params == null)
      params=new JsonObject();
    return params;
  }
  
  public void encodeParams(Object obj){
    if(params!=null)
      throw new IllegalStateException("Params already set! "+params);
    if(obj instanceof JsonObject)
      params=((JsonObject) obj).copy();
    else{
      paramsClassname=obj.getClass().getName();
      params=new JsonObject(Json.encode(obj));
    }
  }

  @SuppressWarnings("unchecked")
  public <C> C reifyParams() throws DecodeException, ClassNotFoundException {
    if (params == null)
      return null;
    checkNotNull(paramsClassname);
    return (C) Json.decodeValue(params.encode(), Class.forName(paramsClassname));
  }


}
