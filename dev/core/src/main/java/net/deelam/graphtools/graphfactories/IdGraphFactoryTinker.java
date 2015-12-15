package net.deelam.graphtools.graphfactories;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphUri;
import net.deelam.graphtools.IdGraphFactory;
import net.deelam.graphtools.PrettyPrintXml;
import net.deelam.graphtools.graphfactories.IdGraphFactoryOrientdb.DB_TYPE;

import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph.FileType;
import com.tinkerpop.blueprints.util.GraphHelper;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

@Slf4j
public class IdGraphFactoryTinker implements IdGraphFactory {
  
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
  
  public void shutdown(GraphUri gUri, IdGraph<?> graph) throws IOException{
    graph.shutdown();
    
    if(prettify && getFileSaveType(gUri) == TinkerGraph.FileType.GRAPHML){
      String graphmlFile = gUri.getUriPath()+"/tinkergraph.xml";
      PrettyPrintXml.prettyPrint(graphmlFile, gUri.getUriPath() + ".graphml");
    }
  }

  private boolean prettify=false;
  private FileType getFileSaveType(GraphUri gUri) {
    // check for secondary scheme
    URI uri = gUri.getUri();
    String fileTypeStr = uri.getScheme(); // enables "tinker:graphml:./target/tGraphML"s

    String path;
    if (fileTypeStr == null) {
      path = gUri.getUriPath();
      if (path == null || path.length() < 1 || path.equals("/")) { // in-memory
        return null;
      }
      fileTypeStr = gUri.getConfig().getString("fileType");
    }

    // not in-memory; storing to disk
    FileType fileType = TinkerGraph.FileType.JAVA; // default type
    if (fileTypeStr != null) {
      if(fileTypeStr.equalsIgnoreCase("prettyGraphml")){
        prettify=true;
        fileType = TinkerGraph.FileType.GRAPHML;
      }else{
        fileType = TinkerGraph.FileType.valueOf(fileTypeStr.toUpperCase());
      }
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
  public void copy(GraphUri srcGraphUri, GraphUri dstGraphUri) throws IOException {
    FileType fileType = getFileSaveType(srcGraphUri);
    if(fileType==null){ // in-memory
      dstGraphUri.createNewIdGraph(false); // to be safe, don't overwrite dst
      GraphHelper.copyGraph(srcGraphUri.getGraph(), dstGraphUri.getGraph());
      return;
    }
    
    File srcFile = new File(srcGraphUri.getUriPath());
    File destFile = new File(dstGraphUri.getUriPath());
    FileUtils.copyDirectory(srcFile, destFile);
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