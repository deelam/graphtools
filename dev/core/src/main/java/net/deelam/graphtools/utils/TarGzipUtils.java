package net.deelam.graphtools.utils;

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
    File file = new File(tarGzPath);
    if(file.exists())
      throw new IllegalArgumentException("File already exists: "+file.getAbsolutePath());
    log.info("compressDirectory {} {}", dirPath, tarGzPath);
    try (TarArchiveOutputStream tOut = new TarArchiveOutputStream(
        new GzipCompressorOutputStream(new BufferedOutputStream(new FileOutputStream(file))))) {
      addFileToTarGz(tOut, new File(dirPath), "");
      tOut.finish();
    }
  }

  private static void addFileToTarGz(TarArchiveOutputStream tOut, File f, String base)
      throws IOException {
    String entryName = base + f.getName();
    TarArchiveEntry tarEntry = new TarArchiveEntry(f, entryName);
    tOut.putArchiveEntry(tarEntry);

    if (f.isFile()) {
      IOUtils.copy(new FileInputStream(f), tOut);
      tOut.closeArchiveEntry();
    } else {
      tOut.closeArchiveEntry();
      File[] children = f.listFiles();
      if (children != null) {
        for (File child : children) {
          addFileToTarGz(tOut, child.getAbsoluteFile(), entryName + "/");
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
          parent.mkdirs();
        }
        if(!overwrite && curfile.exists())
          throw new IllegalArgumentException("Destination directory already exists: "+curfile.getAbsolutePath());
        try (OutputStream out = new FileOutputStream(curfile)) {
          IOUtils.copy(tIn, out);
        }
        entry = tIn.getNextTarEntry();
      }
    }
  }
  

  public static void main(String[] args) throws IOException {
    compressDirectory("target/classes", "classes.tgz");
    uncompressDirectory("classes.tgz", ".", false);
  }
}

