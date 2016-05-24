package net.deelam.vertx.pool;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;

@Slf4j
public final class SerDeserUtils {
  
  public static PondVerticle.Serializer createGraphUriSerializer() {
    return (URI origUri, String localPondDir) -> {
      try {
        File serFile=new File(localPondDir, getSerializedFilename(origUri));
        TarGzipUtils.compressDirectory(new GraphUri(origUri.toString()).getUriPath(), serFile.getAbsolutePath());
        return serFile.toPath();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  public static PondVerticle.Deserializer createGraphUriDeserializer() {
    return (URI origUri, Path localSerializedFile, String localPondDir) -> {
      try {
        //log.info("{} {} {}", origUri, localSerializedFile, localPondDir);
        File outFile = new File(localPondDir, getSerializedFilename(origUri));
        TarGzipUtils.uncompressDirectory(localSerializedFile.toAbsolutePath().toString(), outFile.getAbsolutePath(),
            false);
        return outFile.toURI();
      } catch (Exception e) {
        log.error("Cannot deserialize given: "+origUri+" "+localSerializedFile+" "+localPondDir, e);
        throw new RuntimeException(e);
      }
    };
  }
  
  private static String getSerializedFilename(URI resourceId) {
    GraphUri gUri=new GraphUri(resourceId.toString());
    return resourceId.getScheme() + "_" + gUri.getUriPath().replace('/', '-')
        + "-" + System.currentTimeMillis();
  }

}

