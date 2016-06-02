package net.deelam.vertx.pool;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;

@Slf4j
public final class SerDeserUtils {
  
  public static void main(String[] args) {
    PondVerticle.Serializer ser=createDirectoryUriSerializer();
    PondVerticle.Deserializer deser=createDirectoryUriDeserializer();
    
    String localPondDir="/home/dd/dev/pondtest";
    new File(localPondDir).mkdirs();
    URI uri=URI.create("lucene:/home/dd/dev/query2-491307018-1897g/indexer");
//    File dir=new File(uri.getPath());
//    System.out.println(dir+" exists? "+dir.exists());
//    File[] files = new File(uri.getPath()).listFiles();
//    System.out.println("files="+files+" at "+new File(uri.getPath()));
    
    Path serPath = ser.apply(uri, localPondDir);
    System.out.println("serPath="+serPath);
    URI copyUri = deser.apply(uri, serPath, localPondDir);
    System.out.println("copyPath="+copyUri);
  }
  
  public static PondVerticle.Serializer createDirectoryUriSerializer() {
    return (URI origUri, String localPondDir) -> {
      try {
        File serFile=new File(localPondDir, getSerializedFilenameForDirectory(origUri) + ".tgz");
        String path=origUri.getPath();
        TarGzipUtils.compressDirectory(path, serFile.getAbsolutePath(), new File(path).getName());
        return serFile.toPath();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  public static PondVerticle.Deserializer createDirectoryUriDeserializer() {
    return (URI origUri, Path localSerializedFile, String localPondDir) -> {
      try {
        //log.info("{} {} {}", origUri, localSerializedFile, localPondDir);
        File outFile = new File(localPondDir, getSerializedFilenameForDirectory(origUri));
        TarGzipUtils.uncompressDirectory(localSerializedFile.toAbsolutePath().toString(), outFile.getAbsolutePath(),
            false);
        URI newUri = new URI(origUri.getScheme(), origUri.getAuthority(), 
            outFile.getAbsolutePath(), origUri.getQuery(), origUri.getFragment());
        log.info("new uri={}", newUri);
        return newUri;
      } catch (Exception e) {
        log.error("Cannot deserialize given: "+origUri+" "+localSerializedFile+" "+localPondDir, e);
        throw new RuntimeException(e);
      }
    };
  }
  
  public static PondVerticle.Serializer createGraphUriSerializer() {
    return (URI origUri, String localPondDir) -> {
      try {
        File serFile=new File(localPondDir, getSerializedFilenameForGraphUri(origUri) + ".tgz");
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
        File outFile = new File(localPondDir, getSerializedFilenameForGraphUri(origUri));
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
  

  private static String getSerializedFilenameForDirectory(URI resourceId) {
    String path=resourceId.getPath();
    if(path.startsWith("./"))
      path=path.substring(2);
    Integer version = PondVerticle.getVersion(resourceId);
    return resourceId.getScheme() + "-" + path.replace('/', '_').replace('?', '_')
        + ((version==null)?"-":"-" + version + "-")
        + System.currentTimeMillis();
  }
  
  private static String getSerializedFilenameForGraphUri(URI resourceId) {
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

