package net.deelam.vertx.pool;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;

@Slf4j
public final class SerDeserUtils {
  
  public static PondVerticle.Serializer createGraphUriSerializer() {
    return (URI origUri, String localPondDir) -> {
      try {
        File serFile=new File(localPondDir, getSerializedFilename(origUri));
        String path=new GraphUri(origUri.toString()).getUriPath();
        TarGzipUtils.compressDirectory(path, serFile.getAbsolutePath(), new File(path).getName());
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
        URI newUri = new URI(origUri.getScheme(), origUri.getAuthority(), 
            outFile.getAbsolutePath(), origUri.getQuery(), origUri.getFragment());
        log.info("new graphUri={}", newUri);
        return newUri;
      } catch (Exception e) {
        log.error("Cannot deserialize given: "+origUri+" "+localSerializedFile+" "+localPondDir, e);
        throw new RuntimeException(e);
      }
    };
  }
  
  private static String getSerializedFilename(URI resourceId) {
    GraphUri gUri=new GraphUri(resourceId.toString());
    String path=gUri.getUriPath();
    if(path.startsWith("./"))
      path=path.substring(2);
    Integer version = PondVerticle.getVersion(resourceId);
    return resourceId.getScheme() + "-" + path.replace('/', '_').replace('?', '_')
        + ((version==null)?"-":"-" + version + "-")
        + System.currentTimeMillis();
  }

}

