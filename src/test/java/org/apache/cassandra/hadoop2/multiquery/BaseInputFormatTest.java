package org.apache.cassandra.hadoop2.multiquery;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the multi-row iterator.
 */
public abstract class BaseInputFormatTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseInputFormatTest.class);

  protected static final String KEYSPACE = "nba";
  protected static final String TABLE_LOGOS = "logos";
  protected static final String TABLE_PLAYERS = "players";

  protected static final String COL_STATE = "state";
  protected static final String COL_CITY = "city";
  protected static final String COL_TEAM = "team";

  protected static final String COL_LOGO = "logo";
  protected static final String COL_PLAYER = "player";

  protected static final int NUM_TEAMS = 8;
  protected static final int NUM_CITIES = 7;
  protected static final int NUM_STATES = 4;

  protected static Map<String, TeamData> mTeams;

  protected static Session mSession;

  private static CassandraDaemon mCassandraDaemon = null;

  protected Configuration mConf;

  /**
   * Use a different native port to avoid conflict with any other local C* clusters (this native
   * port is also specified in the YAML file). /
   */
  private static int NATIVE_PORT;

  private static void startCluster() throws IOException {
    String cassandraAddress = System.getProperty(
        "org.apache.cassandra.hadoop2.NativeInputFormat.CASSANDRA_ADDRESS", null
    );

    if (null == cassandraAddress) {
      startEmbeddedCluster();
    } else {
      connectToRunningCluster(cassandraAddress);
    }
  }

  private static void connectToRunningCluster(String cassandraAddress) throws IOException {
    NATIVE_PORT = 9042;
    Cluster cluster = Cluster.builder().addContactPoint(cassandraAddress).build();
    mSession = cluster.connect();
  }

  private static void deleteCassandraDirectories() {
    ArrayList<String> directoriesToDelete = Lists.newArrayList();
    directoriesToDelete.addAll(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()));
    directoriesToDelete.addAll(Arrays.asList(DatabaseDescriptor.getCommitLogLocation()));
    directoriesToDelete.addAll(Arrays.asList(DatabaseDescriptor.getSavedCachesLocation()));

    try {
      for (String dirName : directoriesToDelete) {
        LOG.debug("Deleting directory " + dirName);
        FileUtils.deleteDirectory(new File(dirName));
      }
      LOG.debug("Deleted directories!");
    } catch (IOException ioe) {
      LOG.warn("Error deleting Cassandra directories!");
    }
  }

  private static void startEmbeddedCluster() throws IOException {
    if (mSession != null) {
      return;
    }
    NATIVE_PORT = 9043;
    try {
      // Use a custom YAML file that specifies different ports from normal for RPC and thrift.
      //File yamlFile = new File(getClass().getResource("/cassandra.yaml").getFile());
      File yamlFile = new File(BaseInputFormatTest.class.getResource("/cassandra.yaml").getFile());

      // TODO: Edit the YAML file to set up different directories, ports, etc. per test class?

      LOG.debug("Starting up embedded cluster!");

      Preconditions.checkArgument(yamlFile.exists());
      System.setProperty("cassandra.config", "file:" + yamlFile.getAbsolutePath());
      System.setProperty("cassandra-foreground", "true");

      // Make sure that all of the directories for the commit log, data, and caches are empty.
      // Thank goodness there are methods to get this information (versus parsing the YAML directly).
      deleteCassandraDirectories();
      DatabaseDescriptor.createAllDirectories();
      deleteCassandraDirectories();
      DatabaseDescriptor.createAllDirectories();

      CommitLog.instance.resetUnsafe();

      LOG.debug("Sleeping for a sec...");
      try {
        Thread.sleep(500);
      } catch (InterruptedException ie) {
      }

      mCassandraDaemon = new CassandraDaemon();
      mCassandraDaemon.init(null);
      mCassandraDaemon.start();

      //EmbeddedCassandraService embeddedCassandraService = new EmbeddedCassandraService();
      //embeddedCassandraService.start();

    } catch (IOException ioe) {
      throw new IOException("Cannot start embedded C* service!");
    }
    Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(NATIVE_PORT).build();
    mSession = cluster.connect();
  }

  private static void createKeyspace() {
    mSession.execute(String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s " +
            "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };",
        KEYSPACE));
  }

  /**
   * Create a table for testing that contains NBA teams and their logos.
   */
  private static void createAndPopulateLogoTable() {
    mSession.execute(String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s (%s text, %s text, %s text, %s text, " +
        "PRIMARY KEY(%s, %s, %s))",
        KEYSPACE, TABLE_LOGOS,
        COL_STATE, COL_CITY, COL_TEAM, COL_LOGO,
        COL_STATE, COL_CITY, COL_TEAM));

    // Let's insert some data for a few teams!
    PreparedStatement preparedStatement = mSession.prepare(String.format(
            "INSERT INTO %s.%s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
            KEYSPACE, TABLE_LOGOS, COL_STATE, COL_CITY, COL_TEAM, COL_LOGO));
    for (TeamData teamData : mTeams.values()) {
      mSession.execute(preparedStatement.bind(
          teamData.getState(),
          teamData.getCity(),
          teamData.getTeamName(),
          teamData.getLogo()
      ));
    }
  }

  private static void createAndPopulatePlayerTable() {
    mSession.execute(String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s (%s text, %s text, %s text, %s text, " +
            "PRIMARY KEY(%s, %s, %s, %s))",
        KEYSPACE, TABLE_PLAYERS,
        COL_STATE, COL_CITY, COL_TEAM, COL_PLAYER,
        COL_STATE, COL_CITY, COL_TEAM, COL_PLAYER));

    // Let's insert some data for a few teams!
    PreparedStatement preparedStatement = mSession.prepare(String.format(
        "INSERT INTO %s.%s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
        KEYSPACE, TABLE_PLAYERS, COL_STATE, COL_CITY, COL_TEAM, COL_PLAYER));
    for (TeamData teamData : mTeams.values()) {
      for (String player : teamData.getPlayers()) {
        mSession.execute(preparedStatement.bind(
            teamData.getState(),
            teamData.getCity(),
            teamData.getTeamName(),
            player
        ));
      }
    }
  }

  private static void createTeams() {
    mTeams = Maps.newHashMap();
    mTeams.put("Bulls", new TeamData("IL", "Chicago", "Bulls", "Bull"));
    mTeams.put("Wizards", new TeamData("DC", "Washington", "Wizards", "Wizard"));
    mTeams.put("Warriors", new TeamData("CA", "Oakland", "Warriors", "Golden Gate Bridge"));
    mTeams.put("Clippers", new TeamData("CA", "Los Angeles", "Clippers", "Boat"));
    mTeams.put("Lakers", new TeamData("CA", "Los Angeles", "Lakers", "Big L"));
    mTeams.put("Mavericks", new TeamData("TX", "Dallas", "Mavericks", "Horse"));
    mTeams.put("Spurs", new TeamData("TX", "San Antonio", "Spurs", "Spur"));
    mTeams.put("Rockets", new TeamData("TX", "Houston", "Rockets", "Big H"));
    Preconditions.checkArgument(NUM_TEAMS == mTeams.size());

    mTeams.get("Bulls").addPlayer("Noah");
    mTeams.get("Bulls").addPlayer("Butler");
    mTeams.get("Wizards").addPlayer("Wall");
    mTeams.get("Wizards").addPlayer("Beal");
    mTeams.get("Clippers").addPlayer("Paul");
    mTeams.get("Clippers").addPlayer("Griffin");
    mTeams.get("Lakers").addPlayer("Kobe");
    mTeams.get("Lakers").addPlayer("Gasol");
    mTeams.get("Mavericks").addPlayer("Dirk");
    mTeams.get("Mavericks").addPlayer("Monte");
    mTeams.get("Spurs").addPlayer("Duncan");
    mTeams.get("Spurs").addPlayer("Parker");
    mTeams.get("Rockets").addPlayer("Harden");
    mTeams.get("Rockets").addPlayer("Howard");
  }

  @BeforeClass
  public static void setupTest() throws IOException {
    createTeams();
    startCluster();
    createKeyspace();
    createAndPopulateLogoTable();
    createAndPopulatePlayerTable();
  }

  @Before
  public void setupConf() {
    mConf = new Configuration();
    ConfigHelper.setInputNativeTransportContactPoints(mConf, "127.0.0.1");
    ConfigHelper.setInputNativeTransportPort(mConf, NATIVE_PORT);
  }

  @AfterClass
  public static void shutdown() {
    // Just drop the keyspace.
    Preconditions.checkNotNull(mSession);
    mSession.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
    /*
    Cluster cluster = mSession.getCluster();
    mSession.close();
    cluster.close();
    Preconditions.checkArgument(cluster.isClosed());
    mCassandraDaemon.deactivate();
    LOG.debug("Shut everything down...");
    deleteCassandraDirectories();
    DatabaseDescriptor.createAllDirectories();
    deleteCassandraDirectories();
    DatabaseDescriptor.createAllDirectories();
    */
  }

  private static class TeamData {
    private final String mState;
    private final String mCity;
    private final String mTeamName;
    private final String mLogo;
    private Set<String> mPlayers;

    // TODO: List of players.

    public TeamData(String state, String city, String teamName, String logo) {
      mState = state;
      mCity = city;
      mTeamName = teamName;
      mLogo = logo;
      mPlayers = Sets.newHashSet();
    }

    public void addPlayer(String player) {
      mPlayers.add(player);
    }

    public String getState() { return mState; }
    public String getCity() { return mCity; }
    public String getTeamName() { return mTeamName; }
    public String getLogo() { return mLogo; }
    public Set<String> getPlayers() { return mPlayers; }
  }
}
