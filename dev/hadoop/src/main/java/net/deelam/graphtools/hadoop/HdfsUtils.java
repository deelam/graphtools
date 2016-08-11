package net.deelam.graphtools.hadoop;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class HdfsUtils {

  private Configuration hadoopConf;

  public HdfsUtils(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  synchronized Connection getHBaseConnection() throws IOException {
    { // To avoid IOException, https://github.com/brianfrankcooper/YCSB/issues/446
      // Doesn't stop error: hadoopConf.unset("hbase.dynamic.jars.dir"); // not needed for HDFS client
      //log.error("dynDir={} fileImpl={}", hadoopConf.get("hbase.dynamic.jars.dir"), hadoopConf.get("fs.file.impl"));
      //HadoopConfigurationHelper.print(hadoopConf, "conf4hbase.xml");
    }
    final Connection connection = ConnectionFactory.createConnection(hadoopConf);
    return connection;
  }

  public URI uploadFileToHdfsIfNeeded(URI sourceUri, String destDir) throws IOException, URISyntaxException {
    URI hdfsUri = sourceUri;
    // copy data to HDFS
    if (sourceUri.getScheme() == null || sourceUri.getScheme().equals("file")) {
      hdfsUri = uploadFile(sourceUri, destDir, true).toUri();
    }
    if (!hdfsUri.getScheme().equals("hdfs"))
      log.error("Expecting 'hdfs' URI scheme but got {} for {}", hdfsUri.getScheme(), hdfsUri);
    return hdfsUri;
  }

  public Path uploadFile(String srcFile, String target, boolean overwrite) throws IOException {
    File file = new File(srcFile);
    return uploadFile(hadoopConf, file, new Path(target), overwrite);
  }

  public Path uploadFile(URI srcFile, String target, boolean overwrite) throws IOException {
    File file = new File(srcFile);
    return uploadFile(hadoopConf, file, new Path(target), overwrite);
  }

  // return String instead of hadoop-specific Path
  public String uploadFile2(String srcFile, String target, boolean overwrite) throws IOException {
    return uploadFile(srcFile, target, overwrite).toString();
  }

  public File downloadFile(String src, String dst) throws IOException {
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      fs.copyToLocalFile(false, new Path(src), new Path(dst), true);
      return new File(dst);
    }
  }

  public static Path uploadFile(Configuration hadoopConf, File file, Path dest, boolean overwrite) throws IOException {
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      if (!fs.exists(dest)) {
        log.info("Creating directory: " + dest + " in workingDir=" + fs.getWorkingDirectory());
        if (!fs.mkdirs(dest)) {
          log.error("Could not create directory: " + dest + ".");
          throw new RuntimeException("Could not create directory: " + dest);
        }
      }

      Path src = new Path(file.getAbsolutePath());
      log.info("Copying " + src + " to hdfs: " + dest);
      fs.copyFromLocalFile(false, overwrite, src, dest);
      Path dstFile = new Path(dest, file.getName());
      Path qualPath = fs.makeQualified(dstFile);
      log.debug("   dstFile={}", qualPath);
      return qualPath;
    }
  }

  public static Iterable<Path> uploadFiles(Configuration hadoopConf, Path dest, File... files)
      throws IOException {
    List<Path> paths = Lists.newArrayList();
    for (File f : files) {
      Path qualPath = uploadFile(hadoopConf, f, dest, true);
      paths.add(qualPath);
    }
    return paths;
  }

  public boolean deleteFile(String hdfsTarget) throws IOException {
    Path target = new Path(hdfsTarget);
    try (FileSystem fs = FileSystem.get(hadoopConf)) {
      if (fs.exists(target) || hdfsTarget.contains("*") || hdfsTarget.contains("?")) {
        log.info("Deleting '" + target + "' in workingDir=" + fs.getWorkingDirectory());
        fs.delete(target, true); // delete file, true for recursive 
        return true;
      } else {
        log.warn("Not deleting; target does not exist: " + target + "  WorkingDir=" + fs.getWorkingDirectory());
      }
      return false;
    }
  }

  public String[] getTablenames() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    //Configuration hadoopConf=GetConfigurationsHelper.getHadoopConfig();
    try (Connection utils = getHBaseConnection()) {
      try (Admin admin = utils.getAdmin()) {
        TableName[] tns = admin.listTableNames();
        String[] names = new String[tns.length];
        for (int i = 0; i < tns.length; ++i) {
          names[i] = tns[i].toString();
        }
        return names;
      }
    }
  }

  public boolean hasTable(String tablename) throws IOException {
    checkNotNull(tablename);
    try (Connection utils = getHBaseConnection()) {
      try (Admin admin = utils.getAdmin()) {
        return admin.tableExists(TableName.valueOf(tablename));
      }
    }
    //Not working: HTableDescriptor[] tables=utils.listTables(tablename); tables.length > 0;
    //		for(String tn: getTablenames()){
    //			if(tablename.equals(tablename))
    //				return true;
    //		}
    //		return false;
  }

  public boolean deleteHBaseTable(String tablename) throws IOException {
    if (hasTable(tablename)) {
      try (Connection utils = getHBaseConnection()) {
        log.debug("Deleting table: " + tablename);
        TableName tn = TableName.valueOf(tablename);
        try (Admin admin = utils.getAdmin()) {
          admin.disableTable(tn);
          admin.deleteTable(tn);
        }
        return true;
      }
    } else {
      log.info("Table does not exists: '{}'", tablename);
    }
    return false;
  }

}
