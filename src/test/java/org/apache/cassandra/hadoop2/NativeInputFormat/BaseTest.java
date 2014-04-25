package org.apache.cassandra.hadoop2.NativeInputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import com.datastax.driver.core.Cluster;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

/**
 * Base class for all unit tests.  Handles setting up / tearing down embedded Cassandra.
 */
public class BaseTest {
  protected Cluster mCassandraCluster;

  /**
   * Ensure that the EmbeddedCassandraService for unit tests is running.  If it is not, then start it.
   */
  @Before
  public void startEmbeddedCassandraServiceIfNotRunningAndOpenSession() throws Exception {
    try {
      // Use a custom YAML file that specifies different ports from normal for RPC and thrift.
      //File yamlFile = new File(getClass().getResource("/cassandra.yaml").getFile());
      File yamlFile = new File(BaseTest.class.getResource("/cassandra.yaml").getFile());

      assert (yamlFile.exists());
      System.setProperty("cassandra.config", "file:" + yamlFile.getAbsolutePath());
      System.setProperty("cassandra-foreground", "true");

      // Make sure that all of the directories for the commit log, data, and caches are empty.
      // Thank goodness there are methods to get this information (versus parsing the YAML directly).
      ArrayList<String> directoriesToDelete = new ArrayList<String>(Arrays.asList(
          DatabaseDescriptor.getAllDataFileLocations()
      ));
      directoriesToDelete.add(DatabaseDescriptor.getCommitLogLocation());
      directoriesToDelete.add(DatabaseDescriptor.getSavedCachesLocation());
      for (String dirName : directoriesToDelete) {
        FileUtils.deleteDirectory(new File(dirName));
      }
      EmbeddedCassandraService embeddedCassandraService = new EmbeddedCassandraService();
      embeddedCassandraService.start();

    } catch (IOException ioe) {
      throw new IOException("Cannot start embedded C* service!");
    }

    // Use different port from normal here to avoid conflicts with any locally-running C* cluster.
    // Port settings are controlled in "cassandra.yaml" in test resources.
    String hostIp = "127.0.0.1";
    int port = 9043;
    mCassandraCluster = Cluster.builder().addContactPoints(hostIp).withPort(port).build();
  }

  @After
  public void shutdown() {
    mCassandraCluster.close();
  }
}
