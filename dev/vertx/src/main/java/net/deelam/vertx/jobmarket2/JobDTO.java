package net.deelam.vertx.jobmarket2;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import net.deelam.common.Pojo;

@Accessors(chain = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Data
@ToString
public class JobDTO {
  String id;
  String type;

  public JobDTO(String id, String type) {
    this.id = id;
    this.type = type;
  }

  String inputPath, outputPath;

  String requesterAddr; // Vertx eventbus address; job worker can register itself to this address
  int progressPollInterval;

  Pojo<?> request; // job-specific parameters

  public JobDTO copy() {
    JobDTO dto = new JobDTO(id, type)
        .setInputPath(inputPath).setOutputPath(outputPath)
        .setRequesterAddr(requesterAddr)
        .setProgressPollInterval(progressPollInterval);

    if (request != null)
      dto.request = request.copy();

    return dto;
  }

}
