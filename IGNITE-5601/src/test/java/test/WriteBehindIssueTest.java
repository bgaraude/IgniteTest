package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.cache.configuration.Factory;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WriteBehindIssueTest {

	private static final String CACHE_NAME = "foo-cache";

	@Rule
	public ExpectedException exception = ExpectedException.none();

	DataSourceFactory dataSourceFactory;

	@Before
	public void beforeClass() throws SQLException {

		dataSourceFactory = new DataSourceFactory();

		System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "fasle");
		IgniteConfiguration config = new IgniteConfiguration();
		Ignition.start(config);

	}

	@After
	public void afterClass() {
		Ignition.stop(true);
		dataSourceFactory = null;
	}

	/**
	 * This case performs write-behind with a correct config; it works as
	 * expected.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void testWriteFoo_WriteBehind_OK() throws SQLException, InterruptedException {

		Ignition.ignite().createCache(createConfiguration(true, false));

		IgniteCache<Object, Object> cache = Ignition.ignite().cache(CACHE_NAME);

		FooKey key = new FooKey(1L);
		Foo value = new Foo("abcd", "1234");

		cache.put(key, value);

		Ignition.ignite().destroyCache(CACHE_NAME);
		
		assertDbContainFoo(1L, "abcd", "1234");

	}

	/**
	 * This case performs write-through with a wrong config; it fails and the
	 * put() returns the SQLException
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void testWriteFoo_WriteThrough_ERROR() throws SQLException, InterruptedException {
		exception.expect(CacheWriterException.class);
		
		// These matchers just match a deep cause or suppressed exception 
		exception.expect(new HasDeepCauseMatcher(CoreMatchers.instanceOf(SQLException.class)));
		exception.expect(new HasDeepCauseMatcher(ThrowableMessageMatcher
				.hasMessage(CoreMatchers.containsString("'UNEXISTING_COLUMN' is not a column"))));

		Ignition.ignite().createCache(createConfiguration(false, true));

		IgniteCache<Object, Object> cache = Ignition.ignite().cache(CACHE_NAME);

		FooKey key = new FooKey(1L);
		Foo value = new Foo("abcd", "1234");

		cache.put(key, value);

	}

	/**
	 * This case performs write-behind with a wrong config.
	 * 
	 * We have no error, but the db contains nothing.
	 * 
	 * @throws SQLException
	 * @throws InterruptedException
	 */
	@Test
	public void testWriteFoo_WriteBehind_ERROR() throws SQLException, InterruptedException {

		Ignition.ignite().createCache(createConfiguration(true, true));

		IgniteCache<Object, Object> cache = Ignition.ignite().cache(CACHE_NAME);

		FooKey key = new FooKey(1L);
		Foo value = new Foo("abcd", "1234");

		cache.put(key, value);
		
		Ignition.ignite().destroyCache(CACHE_NAME);
		
		
		// The following assertion fails, and we have no error. The original cause (SQLException) appears nowhere.
		// The logs (with QUIET=false) are just saying:
		//[2017-06-28 10:48:52] SEVERE: Unable to update underlying store: CacheJdbcPojoStore []
		//[2017-06-28 10:48:52] WARNING: Failed to update store (value will be lost as current buffer size is greater than 'cacheCriticalSize' or node has been stopped before store was repaired) [key=test.WriteBehindIssue$FooKey [idHash=524852907, hash=-1388553726, id=1], val=test.WriteBehindIssue$Foo [idHash=323347362, hash=574522785, value1=abcd, value2=1234], op=PUT]
		assertDbContainFoo(1L, "abcd", "1234");
	}
	
	void assertDbContainFoo(long id, String value1, String value2) throws SQLException{

		try (Connection conn = dataSourceFactory.create().getConnection();
				PreparedStatement stmt = conn
						.prepareStatement("SELECT ID, VALUE_1, VALUE_2 FROM DERBY.FOO WHERE ID = ?")) {
			stmt.setLong(1, 1L);
			try (ResultSet rs = stmt.executeQuery()) {

				assertTrue("ResultSet must have at least one record", rs.next());
				assertEquals(1L, rs.getLong("ID"));
				assertEquals("abcd", rs.getString("VALUE_1"));
				assertEquals("1234", rs.getString("VALUE_2"));

			}
		}
	}
	
	CacheConfiguration<FooKey, Foo> createConfiguration(boolean writeBehind, boolean erroneous) {
		CacheConfiguration<FooKey, Foo> cacheConfig = new CacheConfiguration<>();
		cacheConfig.setTypes(FooKey.class, Foo.class);
		cacheConfig.setName(CACHE_NAME);
		cacheConfig.setIndexedTypes(FooKey.class, Foo.class);
		cacheConfig.setWriteThrough(true);
		cacheConfig.setWriteBehindEnabled(writeBehind);
		if (writeBehind) {
			cacheConfig.setWriteBehindFlushThreadCount(1);
			cacheConfig.setWriteBehindFlushFrequency(100L);
		}
		CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();
		storeFactory.setDataSourceFactory(dataSourceFactory);

		storeFactory.setTypes(fooType(CACHE_NAME, erroneous));
		cacheConfig.setCacheStoreFactory(storeFactory);
		return cacheConfig;
	}

	/**
	 * 
	 * @param cacheName
	 * @param erroneous
	 *            If true, generate an erroneous JdbcType with an unexisting
	 *            column.
	 * @return
	 */
	static JdbcType fooType(String cacheName, boolean erroneous) {
		JdbcType jdbcType = new JdbcType();
		jdbcType.setCacheName(cacheName);

		jdbcType.setDatabaseSchema("DERBY");
		jdbcType.setDatabaseTable("FOO");
		jdbcType.setKeyType(FooKey.class.getName());
		jdbcType.setValueType(Foo.class.getName());

		List<JdbcTypeField> keys = new ArrayList<>();
		keys.add(new JdbcTypeField(java.sql.Types.INTEGER, "ID", Long.class, "id"));
		jdbcType.setKeyFields(keys.toArray(new JdbcTypeField[0]));

		List<JdbcTypeField> values = new ArrayList<>();
		values.add(new JdbcTypeField(java.sql.Types.VARCHAR, "VALUE_1", String.class, "value1"));
		values.add(new JdbcTypeField(java.sql.Types.VARCHAR, erroneous ? "UNEXISTING_COLUMN" : "VALUE_2", String.class,
				"value2"));
		jdbcType.setValueFields(values.toArray(new JdbcTypeField[0]));

		return jdbcType;
	}

	static final class Foo {

		@QuerySqlField
		String value1;

		@QuerySqlField
		String value2;

		public Foo() {
		}

		public Foo(String value1, String value2) {
			this.value1 = value1;
			this.value2 = value2;
		}

	}

	static final class FooKey {

		long id;

		public FooKey() {

		}

		public FooKey(long id) {
			this.id = id;
		}

		@Override
		public int hashCode() {
			return Long.hashCode(id);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (FooKey.class.equals(obj.getClass()))
				return false;
			return id == ((FooKey) obj).id;
		}
	}

	static final class DataSourceFactory implements Factory<DataSource> {

		private static final long serialVersionUID = 1L;

		final EmbeddedDataSource dataSource;

		public DataSourceFactory() throws SQLException {
			dataSource = new EmbeddedDataSource();
			dataSource.setDatabaseName("DERBY");
			dataSource.setCreateDatabase("create");
			try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
				stmt.execute(
						"CREATE TABLE DERBY.FOO (ID INT, VALUE_1 VARCHAR(32), VALUE_2 VARCHAR(32), PRIMARY KEY (ID))");
			} catch (SQLException e) {
				// X0Y32 means the table already exists.
				if (!"X0Y32".equals(e.getSQLState())) {
					throw e;
				}
				System.out.printf("Table %s already exists.%n", "FOO");
			}
			try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
				stmt.execute("DELETE FROM DERBY.FOO");
			}
		}

		@Override
		public DataSource create() {
			return dataSource;
		}
	}
}
