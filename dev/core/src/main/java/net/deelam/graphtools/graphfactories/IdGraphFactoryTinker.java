package net.deelam.graphtools.graphfactories;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.IdGraphFactory;

import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph.FileType;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

@Slf4j
public final class IdGraphFactoryTinker implements IdGraphFactory {
  
  public static void register() {
    GraphUri.register("tinker", new IdGraphFactoryTinker());
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public IdGraph<TinkerGraph> open(GraphUri gUri) {
    // check desired output format
    FileType fileType = getFileSaveType(gUri);
    
    // open graph
    IdGraph<TinkerGraph> graph;
    if (fileType==null) {
      log.debug("Opening Tinker graph in memory");
      graph = new IdGraph<>(new TinkerGraph());
    } else {
      String path = gUri.getUriPath();
      log.debug("Opening Tinker graph at path={} of type={}", path, fileType);
      graph = new IdGraph<>(new TinkerGraph(path, fileType));
    }
    return graph;
  }

  private static FileType getFileSaveType(GraphUri gUri) {
    String path = gUri.getUriPath();
    if (path == null || path.length()<1 || path.equals("/")) { // in-memory
      return null;
    }
    FileType fileType = TinkerGraph.FileType.JAVA;
    String fileTypeStr = gUri.getConfig().getString("fileType");
    if (fileTypeStr != null) {
      fileType = TinkerGraph.FileType.valueOf(fileTypeStr.toUpperCase());
    }
    return fileType;
  }

  @Override
  public void delete(GraphUri gUri) throws IOException {
    FileType fileType = getFileSaveType(gUri);
    if(fileType==null){ // in-memory
      return;
    }
    File pathFile = new File(gUri.getUriPath());
    log.info("Deleting TinkerGraph at {}",pathFile);
    FileUtils.deleteDirectory(pathFile);
  }

  @Override
  public boolean exists(GraphUri gUri) {
    FileType fileType = getFileSaveType(gUri);
    if(fileType==null){ // in-memory
      return false;
    }
    File pathFile = new File(gUri.getUriPath());
    return pathFile.exists();
  }
  
}