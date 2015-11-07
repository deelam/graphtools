package net.deelam.graphtools.importer.csv;


public class CsvUtils {
  public static String getFirstField(String rowStr, char delimiter) {
    int firstDelim = rowStr.indexOf(delimiter);
    if (firstDelim < 0)
      return null;
    String field1 = rowStr.substring(0, firstDelim).trim();
    return field1;
  };

}
