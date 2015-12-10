package net.deelam.enricher.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import net.deelam.graphtools.GraphRecord;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.*;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;

@Slf4j
public class NodeIndexer implements AutoCloseable {

  public static final String NODE_ID_FIELD = "nodeId";
  public static final String SRC_GRAPH_FIELD = "srcGraph";

  private Directory dir;

  public NodeIndexer(Directory dir) throws IOException {
    this.dir = dir;
  }

  @Override
  public void close() throws IOException {
    dir.close();
  }

  public void indexGraph(Graph graph, String inputGraphname) throws IOException {
    //    The same analyzer should be used for indexing and searching
    PerFieldAnalyzerWrapper analyzers =
        new PerFieldAnalyzerWrapper(new StandardAnalyzer(), analyzerMap);
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

  List<EntityIndexer> eIndexers = new ArrayList<>();

  public void registerEntityIndexer(EntityIndexer indexer) {
    eIndexers.add(indexer);
    analyzerMap.putAll(indexer.createAnalyzers());
  }

  private Map<String, Analyzer> analyzerMap = new HashMap<>();

  public void addAnalyzer(String fieldName, Analyzer anlyzr) {
    analyzerMap.put(fieldName, anlyzr);
  }

  private boolean indexNode(IndexWriter writer, Vertex v, String inputGraphname) throws IOException {
    Document doc = null;
    String type = GraphRecord.getType(v);
    if (type == null)
      return false;

    //      EntityIndexer indexer = eIndexers.get(type);
    //      if (indexer == null) {
    //        log.warn("No indexer for type={}", type);
    //        return false;
    //      }
    doc = new Document();
    for (EntityIndexer indexer : eIndexers) {
      indexer.index(v, doc);
    }

    doc.add(new StringField(SRC_GRAPH_FIELD, inputGraphname, Field.Store.YES));
    doc.add(new StringField(NODE_ID_FIELD, v.getId().toString(), Field.Store.YES));
    writer.addDocument(doc);
    return true;
  }

  public void list(String sortField, String fieldType, String printField, int limit)
      throws IOException, ParseException {
    try (IndexReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      SortField sortFieldInst = getSortField(sortField, fieldType);

      Query queryAll = new FieldValueQuery(sortField); //new MatchAllDocsQuery();
      TopFieldDocs hits = searcher.search(queryAll, Integer.MAX_VALUE, new Sort(sortFieldInst));

      System.out.println("Found " + hits.totalHits + " hits.");
      limit = (limit < 0) ? hits.totalHits : Math.min(limit, hits.totalHits);
      for (int i = 0; i < limit; ++i) {
        int docId = hits.scoreDocs[i].doc;
        Document d = searcher.doc(docId);
        System.out.println((i + 1) + ". " + d.get(printField) + " " + d.get(SRC_GRAPH_FIELD) + "["
            + d.get(NODE_ID_FIELD) + "]");
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

  public void listTerm(String sortField, String fieldType, String printField, int limit)
      throws IOException, ParseException {
    try (IndexReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      SortField sortFieldInst = getSortField(sortField, fieldType);

      Query queryAll = new TermQuery(new Term(sortField)); //FieldValueQuery(sortField); //new MatchAllDocsQuery();
      TopDocs hits = searcher.search(queryAll, Integer.MAX_VALUE /*, new Sort(sortFieldInst)*/);

      System.out.println("Found " + hits.totalHits + " hits.");
      limit = (limit < 0) ? hits.totalHits : Math.min(limit, hits.totalHits);
      for (int i = 0; i < limit; ++i) {
        int docId = hits.scoreDocs[i].doc;
        Document d = searcher.doc(docId);
        System.out.println((i + 1) + ". " + d.get(printField) + " " + d.get(SRC_GRAPH_FIELD) + "["
            + d.get(NODE_ID_FIELD) + "]");
      }
    }
  }

  public void listTermEnum(String tokenField, String printField, int limit) throws IOException, ParseException {
    try (IndexReader reader = DirectoryReader.open(dir)) {

      Fields fields = MultiFields.getFields(reader);
      Terms terms = fields.terms(tokenField);

      TermsEnum termsEnum = terms.iterator();
      BytesRef text;
      while ((text = termsEnum.next()) != null) {
        System.out.println("field=" + tokenField + "; text=" + text.utf8ToString());
        TermQuery tq = new TermQuery(new Term(tokenField, text));
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(tq, limit);

        if (hits.totalHits > 1) {
          System.out.println("Found " + hits.totalHits + " hits.");
          int _limit = (limit < 0) ? hits.totalHits : Math.min(limit, hits.totalHits);
          for (int i = 0; i < _limit; ++i) {
            int docId = hits.scoreDocs[i].doc;
            Document d = searcher.doc(docId);
            System.out.println((i + 1) + ". " + d.get(printField) + " " + d.get(SRC_GRAPH_FIELD) + "["
                + d.get(NODE_ID_FIELD) + "]");
          }
        }
      }
    }
  }

  public void listTermEnum3(String tokenField, String printField, int limit) throws IOException, ParseException {
    try (IndexReader reader = DirectoryReader.open(dir)) {
      Fields fields = MultiFields.getFields(reader);
      Terms terms = fields.terms(tokenField);

      TermsEnum termsEnum = terms.iterator();
      BytesRef text;
      while ((text = termsEnum.next()) != null) {
        System.out.println("field=" + tokenField + "; text=" + text.utf8ToString());
        TermQuery tq = new TermQuery(new Term(tokenField, text));
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(tq, limit);
        if (hits.totalHits > 1) {
          System.out.println("Found " + hits.totalHits + " hits.");
          int _limit = (limit < 0) ? hits.totalHits : Math.min(limit, hits.totalHits);
          for (int i = 0; i < _limit; ++i) {
            int docId = hits.scoreDocs[i].doc;
            Document d = searcher.doc(docId);
            System.out.println((i + 1) + ". " + d.get(printField) + " " + d.get(SRC_GRAPH_FIELD) + "["
                + d.get(NODE_ID_FIELD) + "]");
          }
        }
      }
    }
  }

  public void listTermEnum2(String sortField, String fieldType, String printField, int limit)
      throws IOException, ParseException {
    try (IndexReader reader = DirectoryReader.open(dir)) {
      Fields fields = MultiFields.getFields(reader);
      for (String field : fields) {
        Terms terms = fields.terms(field);
        TermsEnum termsEnum = terms.iterator();
        BytesRef text;
        while ((text = termsEnum.next()) != null) {
          System.out.println("field=" + field + "; text=" + text.utf8ToString());
        }
      }
    }
  }

}
