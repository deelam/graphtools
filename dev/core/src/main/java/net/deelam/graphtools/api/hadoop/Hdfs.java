package net.deelam.graphtools.api.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * @author dnlam
 *
 */
public interface Hdfs {

  CompletableFuture<File> downloadFile(String src, String dst) throws IOException;

}
