package net.deelam.vertx.jobstore;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Job JSON Object
 * @author dd
 *
 */
public class JobJO extends JsonObject {

  public JobJO(String id) {
    put(ID_ATTRIBUTE, id);
  }

  public JobJO(JsonObject obj) {
    super();
    checkNotNull(obj.getString(ID_ATTRIBUTE, null), "Must have '" + ID_ATTRIBUTE + "' attribute: " + obj);
    mergeIn(obj);
  }
  
  ///

  private static final String ID_ATTRIBUTE = "id";

  public String getJobId() {
    return getString(ID_ATTRIBUTE);
  }

  ///
  
  public static final String JOB_COMPLETE_ADDRESS = "jobCompleteAddress";

  public String getJobCompletionAddress() {
    return getString(JOB_COMPLETE_ADDRESS);
  }

  public void setJobCompletionAddress(String jobCompletionAddress) {
    put(JOB_COMPLETE_ADDRESS, jobCompletionAddress);
  }

  ///

  private static final String UPDATE_TYPE = "updateType";

  public void setUpdateType(String updateType) {
    put(UPDATE_TYPE, updateType);
  }

  public String getUpdateType() {
    return getString(UPDATE_TYPE);
  }

  ////

  private static Set<String> propsNoCopy = new HashSet<>();

  static {
    propsNoCopy.add(ID_ATTRIBUTE);
    propsNoCopy.add(UPDATE_TYPE);
  }

  public void updateState(JsonObject job) {
    job.forEach(entry -> {
      if (!propsNoCopy.contains(entry.getKey()))
        put(entry.getKey(), entry.getValue());
    });

    put("updatedAt", new Date().toString());
  }

  public static JobJO wrap(Message<Object> message) {
    return new JobJO((JsonObject) message.body());
  }

  public static JobJO wrap(JsonObject obj) {
    return new JobJO(obj);
  }

}
