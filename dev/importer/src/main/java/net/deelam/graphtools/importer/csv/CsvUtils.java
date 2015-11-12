package net.deelam.graphtools.importer.csv;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvUtils {
  public static String getFirstField(String rowStr, char delimiter) {
    int firstDelim = rowStr.indexOf(delimiter);
    if (firstDelim < 0)
      return null;
    String field1 = rowStr.substring(0, firstDelim).trim();
    return field1;
  }

  static boolean shouldIgnore(String rowStr, long lineNum, CsvParser<?> parser) {
    if (rowStr == null)
      return true;
  
    if (rowStr.length() == 0) {
      log.warn("Skipping blank line (at line {})", lineNum);
      return true;
    }
    if (rowStr.startsWith("#")) {
      log.warn("Skipping comment (at line {}): {}", lineNum, rowStr);
      return true;
    }
  
    if (parser.shouldIgnore(rowStr)) {
      log.warn("Ignoring line (at line {}): {}", lineNum, rowStr);
      return true;
    }
    return false;
  };

}
