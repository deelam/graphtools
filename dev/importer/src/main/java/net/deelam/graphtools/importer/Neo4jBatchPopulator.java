package net.deelam.graphtools.importer;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.*;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.*;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

@RequiredArgsConstructor
@Slf4j
public class Neo4jBatchPopulator implements Populator {

  @Getter
  private final String importerName;

  private static final String IMPORTER_KEY = "_ingester";

  private void markRecords(Collection<GraphRecord> gRecords) {
    for (GraphRecord rec : gRecords) {
      rec.setProperty(IMPORTER_KEY, importerName);
      for (Entry<String, Edge> e : rec.getOutEdges().entrySet()) {
        e.getValue().setProperty(IMPORTER_KEY, importerName);
      }
      for (Entry<String, Edge> e : rec.getInEdges().entrySet()) {
        e.getValue().setProperty(IMPORTER_KEY, importerName);
      }
    }
  }

  /**
   * Conclusion: ChronicleMap backed by file is as fast as in-memory HashMap.
   * Performance test results: 
  90k hashmap w/o neoindexing  30s
  900k hashmap w/o neoindexing 4.5mins
  90k hashmap w neoindexing  60s
  900k hashmap w neoindexing 9:12mins 26:08-35:20


  90k mapdb.memDB w/o neoindexing  47s
  90k mapdb.memDB w neoindexing  1:11
  900k mapdb.memDB w neoindexing   39:02-52:22

  90k chronmap w neoindexing 57s 
  900k chronmap w neoindexing  9:40mins 58:41-08:19
  90k chronmap.file w neoindexing  60s
  900k chronmap.file w neoindexing ~10 mins 16:44-26:35
   */
  private Map<String, Long> mapDb = null;
  BatchInserter graph = null;
  private BatchInserterIndex nodeStringIdIndex = null;
  private BatchInserterIndex edgeStringIdIndex = null;

  @Override
  public void populateGraph(GraphUri graphUri, Collection<GraphRecord> gRecords) {
    if (importerName != null) {
      markRecords(gRecords);
    }

    File mapFile;
    try {
      if (mapDb == null) {
        mapFile = File.createTempFile("batchImporter-", ".map");
        log.info("Creating temp file: " + mapFile.getAbsolutePath());
        mapDb = ChronicleMapBuilder.of(String.class, Long.class)
            .entries(1000000).createPersistedTo(mapFile);
      }

      if (graph == null) {
        graphUri.delete();
        graph = BatchInserters.inserter(graphUri.getUriPath());
        Map<String, Object> rootNodeProps=new HashMap<>();
        rootNodeProps.put(IdGraph.ID, "root");
        graph.setNodeProperties(0, rootNodeProps);
        
        BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider(graph);
        nodeStringIdIndex = indexProvider.nodeIndex("stringId", MapUtil.stringMap("type", "exact"));
        nodeStringIdIndex.setCacheCapacity(IdGraph.ID, 100000);

        edgeStringIdIndex = indexProvider.relationshipIndex("stringId", MapUtil.stringMap("type", "exact"));
        edgeStringIdIndex.setCacheCapacity(IdGraph.ID, 100000);
      }
      // add to graph
      for (GraphRecord gr : gRecords) {
        long newVLongId = importVertex(graph, gr); //getLongId(gr.getStringId());    
        importEdges(graph, Direction.OUT, newVLongId, gr);
        importEdges(graph, Direction.IN, newVLongId, gr);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
    }
  }

  public void shutdown() {
    nodeStringIdIndex.flush();
    edgeStringIdIndex.flush();
    graph.shutdown();
  }


  private void importEdges(BatchInserter graph, Direction direction, long newVLongId, GraphRecord gr) {
    for (Edge e : gr.getEdges(direction)) {
      GraphRecord oppV = (GraphRecord) e.getVertex(direction.opposite());
      long newOppVLongId = importVertex(graph, oppV);
      importEdge(graph, (GraphRecordEdge) e, direction, newVLongId, newOppVLongId);
    }
  }

  JsonPropertyMerger jsonPropMerger = new JsonPropertyMerger();

  public long importVertex(BatchInserter graph, GraphRecord gr) {
    String id = gr.getStringId();
    //long longId=getLongId(id);
    Long longId = (Long) mapDb.get(id);
    if (longId == null) {
      Map<String, Object> cProps = jsonPropMerger.convertToJson(gr.getProps());
      cProps.put(IdGraph.ID, id);
      longId = graph.createNode(cProps);
      //log.info("Create node: {} {}",longId, id);
      nodeStringIdIndex.add(longId, cProps);
      mapDb.put(id, longId);
    } else {
      Map<String, Object> existingProps = graph.getNodeProperties(longId);
      //log.info("Found node: {} {}",longId, id);
      Map<String, Object> props = mergePropsIfNeeded(graph, gr, longId, existingProps);
      if (props != null)
        graph.setNodeProperties(longId, props);
    }
    return longId.longValue();
  }

  private Map<String, Object> mergePropsIfNeeded(BatchInserter graph, GraphRecordElement gr, Long longId,
      Map<String, Object> existingProps) {
    if (gr.getProps().isEmpty()) {
      // don't changed existingProps
      return null;
    } else {
      Map<String, Object> props = null;
      if (existingProps.size() > 1) { // check if existingProps has more than the ID property
        props = copyProperties(gr, existingProps);
      } else {
        props = gr.getProps();
      }
      return jsonPropMerger.convertToJson(props);
    }
  }

  //  private long getLongId(String stringId) {
  //    Long longId = (Long) mapDb.get(stringId);
  //    if (longId == null) {
  //      longId = Long.valueOf(mapDb.size());
  //      mapDb.put(stringId, longId);
  //    }
  //    log.info("{}={}",stringId, longId);
  //    return longId.longValue();
  //  }

  public void importEdge(BatchInserter graph, GraphRecordEdge grE, Direction direction,
      long v1inGraphLongId, long v2inGraphLongId) {
    String edgeId = grE.getStringId();
    Long edgeLongId = (Long) mapDb.get(edgeId);
    if (edgeLongId == null) {
      RelationshipType type = DynamicRelationshipType.withName(grE.getLabel());
      Map<String, Object> cProps = jsonPropMerger.convertToJson(grE.getProps());
      cProps.put(IdGraph.ID, edgeId);
      if (direction == Direction.OUT) {
        edgeLongId = graph.createRelationship(v1inGraphLongId, v2inGraphLongId, type, cProps);
      } else {
        edgeLongId = graph.createRelationship(v2inGraphLongId, v1inGraphLongId, type, cProps);
      }
      edgeStringIdIndex.add(edgeLongId, cProps);
    } else {
      BatchRelationship newEdge = graph.getRelationshipById(edgeLongId);
      String label = newEdge.getType().name();
      long v1, v2;
      if (direction == Direction.OUT) {
        v1 = newEdge.getStartNode();
        v2 = newEdge.getEndNode();
      } else {
        v2 = newEdge.getStartNode();
        v1 = newEdge.getEndNode();
      }
      if (!label.equals(grE.getLabel()))
        throw new IllegalArgumentException("Expecting " + grE.getLabel() + " but got "
            + label);
      if (v1 != v1inGraphLongId)
        throw new IllegalArgumentException("Expecting " + v1inGraphLongId + " but got "
            + v1 + " for edge " + edgeId);
      if (v2 != v2inGraphLongId)
        throw new IllegalArgumentException("Expecting " + v2inGraphLongId + " but got "
            + v2 + " for edge " + edgeId);

      Map<String, Object> existingProps = graph.getRelationshipProperties(edgeLongId);
      Map<String, Object> props = mergePropsIfNeeded(graph, grE, edgeLongId, existingProps);
      if (props != null)
        graph.setRelationshipProperties(edgeLongId, props);
    }
  }

  static final String SET_SUFFIX = DefaultGraphRecordMerger.SET_SUFFIX;

  @Getter
  final GraphRecordMerger graphRecordMerger;

  private GraphRecord tempGr = new GraphRecordImpl("temp");

  private Map<String, Object> copyProperties(Element fromE, Map<String, Object> existingProps) {
    //log.info("Copy props from existing element={} \n\t existing={}", fromE, existingProps);
    tempGr.clearProperties();
    jsonPropMerger.convertFromJson(existingProps, tempGr);
    graphRecordMerger.mergeProperties(fromE, tempGr);
    return tempGr.getProps();
  }

}
