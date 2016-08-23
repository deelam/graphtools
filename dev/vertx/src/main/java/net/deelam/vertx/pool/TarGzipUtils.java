package net.deelam.vertx.pool;

import java.io.*;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TarGzipUtils {
  
  public static void compressDirectory(String dirPath, String tarGzPath) throws IOException {
    compressDirectory(dirPath, tarGzPath, null);
  }
  public static void compressDirectory(String dirPath, String tarGzPath, String relativeDir) throws IOException {
    File file = new File(tarGzPath);
    if(file.exists())
      throw new IllegalArgumentException("File already exists: "+file.getAbsolutePath());
    log.info("compressDirectory {} {} relativeDir={}", dirPath, tarGzPath, relativeDir);
    try (TarArchiveOutputStream tOut = new TarArchiveOutputStream(
        new GzipCompressorOutputStream(new BufferedOutputStream(new FileOutputStream(file))))) {
      tOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX); // allow long filenames
      addFileToTarGz(tOut, new File(dirPath), "", relativeDir);
      tOut.finish();
    }
  }

  private static void addFileToTarGz(TarArchiveOutputStream tOut, File f, String entryNameBaseDir, String relativeDir)
      throws IOException {
    String actualEntryName = entryNameBaseDir + f.getName();
    String entryName = actualEntryName;
    if(relativeDir!=null){
      if(entryName.startsWith(relativeDir))
        entryName=entryName.substring(relativeDir.length());
      else if(relativeDir.startsWith(entryName))
        entryName="";
      else {
        log.warn("Excluding file {} because it is not in relativeDir={}",actualEntryName,relativeDir);
        entryName="";
      }
    }
    
    if(entryName.length()>0){
      TarArchiveEntry tarEntry = new TarArchiveEntry(f, entryName);
      tOut.putArchiveEntry(tarEntry);
    }

    if (f.isFile()) {
      if(entryName.length()>0){
        IOUtils.copy(new FileInputStream(f), tOut);
        tOut.closeArchiveEntry();
      }
    } else {
      if(entryName.length()>0)
        tOut.closeArchiveEntry();
      File[] children = f.listFiles();
      if (children != null) {
        for (File child : children) {
          addFileToTarGz(tOut, child.getAbsoluteFile(), actualEntryName + "/", relativeDir);
        }
      }
    }
  }
  
  public static void uncompressDirectory(String tarGzPath, String dirPath, boolean overwrite) throws IOException {
    log.info("uncompressDirectory {} {}", tarGzPath, dirPath);
    File file = new File(tarGzPath);
    try (TarArchiveInputStream tIn =
        new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(file))))) {
      TarArchiveEntry entry = tIn.getNextTarEntry();
      while (entry != null) {
        if (entry.isDirectory()) {
          entry = tIn.getNextTarEntry();
          continue;
        }
        File curfile = new File(dirPath, entry.getName());
        File parent = curfile.getParentFile();
        if (!parent.exists()) {
          if(!parent.mkdirs())
            log.error("Could not create dir={}", parent);
        }
        if(!overwrite && curfile.exists())
          throw new IllegalArgumentException("Destination already exists: "+curfile.getAbsolutePath());
        try (OutputStream out = new FileOutputStream(curfile)) {
          IOUtils.copy(tIn, out);
        }
        entry = tIn.getNextTarEntry();
      }
    }
  }
  

  public static void main(String[] args) throws IOException {
    //compressDirectory("target/classes/net", "classes1.tgz");
    String path="target/classes";
    compressDirectory(path, "classes.tgz", new File(path).getName());
    //compressDirectory(path, "classes.tgz", new File(path).getName()+"/net");
    //uncompressDirectory("classes.tgz", "classes", false);
  }
}

