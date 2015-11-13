package net.deelam.enricher.indexing;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.FieldValueQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

@Slf4j
public class NodeIndexer implements AutoCloseable{

  private static final String NODE_ID_FIELD = "nodeId";
  private static final String SRC_GRAPH_FIELD = "srcGraph";
  
  private Directory dir;

  public NodeIndexer(String path) throws IOException {
    if(path==null){
      dir = new RAMDirectory();
    }else{
      dir=FSDirectory.open(Paths.get(path));
    }
  }
  
  @Override
  public void close() throws IOException {
    dir.close();
  }

  public void indexGraph(Graph graph, String inputGraphname) throws IOException {
    //    The same analyzer should be used for indexing and searching
    IndexWriterConfig config = new IndexWriterConfig(analyzers); //new config for each writer
    int count = 0;
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (Vertex v : graph.getVertices()) {
        if (indexNode(writer, v, inputGraphname))
          ++count;
        if (count % 500 == 0)
          log.debug("  processed " + count + " nodes");
      }
      writer.commit();
    }
    log.info("Indexed " + count + " relevant nodes.");
  }

  Map<String, EntityIndexer> eIndexers = new HashMap<>();

  public void registerEntityIndexer(String entityType, EntityIndexer indexer) {
    eIndexers.put(entityType, indexer);
  }
  
  private Map<String, Analyzer> analyzerMap=new HashMap<>();
  private PerFieldAnalyzerWrapper analyzers = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), analyzerMap);
  public void addAnalyzer(String fieldName, Analyzer anlyzr){
    analyzerMap.put(fieldName, anlyzr);
  }

  private boolean indexNode(IndexWriter writer, Vertex v, String inputGraphname) throws IOException {
    Document doc = null;
    String type = GraphRecord.getType(v);
    if (type == null)
      return false;

    EntityIndexer indexer = eIndexers.get(type);
    if (indexer == null)
      return false;

    doc = indexer.index(v);
    doc.add(new StringField(SRC_GRAPH_FIELD, inputGraphname, Field.Store.YES));
    doc.add(new StringField(NODE_ID_FIELD, v.getId().toString(), Field.Store.YES));
    writer.addDocument(doc);
    return true;
  }

  public void list(String sortField, String fieldType, String printField, int limit) throws IOException,
      ParseException {
    try (IndexReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      SortField sortFieldInst = getSortField(sortField, fieldType);
      
      Query queryAll = new FieldValueQuery(sortField); //new MatchAllDocsQuery();
      TopFieldDocs hits = searcher.search(queryAll, Integer.MAX_VALUE, new Sort(sortFieldInst));

      System.out.println("Found " + hits.totalHits + " hits.");
      limit=(limit<0)?hits.totalHits:limit;
      for (int i = 0; i < limit; ++i) {
        int docId = hits.scoreDocs[i].doc;
        Document d = searcher.doc(docId);
        System.out.println((i + 1) + ". " + d.get(printField) 
            + " " + d.get(SRC_GRAPH_FIELD) + "[" + d.get(NODE_ID_FIELD) 
            + "]");
      }
    }
  }

  private static SortField getSortField(String sortField, String fieldType) {
    Type sortedFieldType = Type.valueOf(fieldType);
    SortField sortFieldInst;
    switch (sortedFieldType) {
      case STRING:
        sortFieldInst = new SortField(sortField, sortedFieldType);
        break;
      case INT:
        sortFieldInst = new SortedNumericSortField(sortField, sortedFieldType);
        break;
      default:
        throw new UnsupportedOperationException("sortedFieldType=" + sortedFieldType);
    }
    return sortFieldInst;
  }
}
