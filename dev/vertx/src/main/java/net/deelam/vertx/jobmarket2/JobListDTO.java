package net.deelam.vertx.jobmarket2;

import java.util.List;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class JobListDTO{
  final List<JobDTO> jobs; // job-specific parameters
}
