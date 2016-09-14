package net.deelam.vertx.jobmarket2;

import io.vertx.core.json.DecodeException;
import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import net.deelam.common.Pojo;

@Accessors(chain=true)
@NoArgsConstructor(access=AccessLevel.PRIVATE)
@Data
public class JobDTO {
  String id;
  String type;
  public JobDTO(String id, String type){
    this.id=id;
    this.type=type;
  }
  
  String inputPath, outputPath;
  
  String requesterAddr; // Vertx eventbus address; job worker can register itself to this address
  int progressPollInterval;

  Pojo<?> request; // job-specific parameters

  public JobDTO copy() {
    JobDTO dto = new JobDTO(id,type)
        .setInputPath(inputPath).setOutputPath(outputPath)
        .setRequesterAddr(requesterAddr)
        .setProgressPollInterval(progressPollInterval)
        ;

    if (request != null)
      dto.request = request.copy();

    return dto;
  }
  
//  public JsonObject getParams() {
//    if (params == null)
//      params=new JsonObject();
//    return params;
//  }
  
  public JobDTO encodeParams(Pojo obj){
    return setRequest(obj);
//    if(params!=null)
//      throw new IllegalStateException("Params already set! "+params);
//    params=params.copy();
//    if(obj instanceof JsonObject)
//    else{
//      
//      paramsClassname=obj.getClass().getName();
//      params=new JsonObject(Json.encode(obj));
//    }
//    return this;
  }

  @SuppressWarnings("unchecked")
  public <C extends Pojo> C decodeParams(ClassLoader cl) throws DecodeException, ClassNotFoundException {
    if (request == null)
      return null;
    return (C) request;
//    if(paramsClassname==null){
//      return (C) params;
//    } else {
//      return (C) Json.decodeValue(params.encode(), cl.loadClass(paramsClassname));
//    }
  }


}
