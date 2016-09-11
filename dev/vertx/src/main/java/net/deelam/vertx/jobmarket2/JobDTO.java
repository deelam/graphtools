package net.deelam.vertx.jobmarket2;

import static com.google.common.base.Preconditions.checkNotNull;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Accessors(chain=true)
@RequiredArgsConstructor
@Data
public class JobDTO {
  final String id;
  final String type;
  
  String inputPath, outputPath;
  
  String requesterAddr; // Vertx eventbus address; job worker can register itself to this address
  int progressPollInterval;

  String paramsClassname;
  JsonObject params; // job-specific parameters

  public JobDTO copy() {
    JobDTO dto = new JobDTO(id,type)
        .setInputPath(inputPath).setOutputPath(outputPath)
        .setRequesterAddr(requesterAddr)
        .setProgressPollInterval(progressPollInterval)
        .setParamsClassname(paramsClassname);

    if (params != null)
      dto.params = params.copy();

    return dto;
  }
  
  public JsonObject getParams() {
    if (params == null)
      params=new JsonObject();
    return params;
  }
  
  public JobDTO encodeParams(Object obj){
    if(params!=null)
      throw new IllegalStateException("Params already set! "+params);
    if(obj instanceof JsonObject)
      params=((JsonObject) obj).copy();
    else{
      paramsClassname=obj.getClass().getName();
      params=new JsonObject(Json.encode(obj));
    }
    return this;
  }

  @SuppressWarnings("unchecked")
  public <C> C reifyParams() throws DecodeException, ClassNotFoundException {
    if (params == null)
      return null;
    checkNotNull(paramsClassname);
    return (C) Json.decodeValue(params.encode(), Class.forName(paramsClassname));
  }


}
