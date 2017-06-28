package test;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SQLQueryOnLocalDateTimeTest {

	private static final String CACHE_NAME = "foo-cache";

	@Before
	public void beforeClass() {
		Ignite ignite = Ignition.start();

		CacheConfiguration<FooKey, Foo> cacheConfig = new CacheConfiguration<>();
		cacheConfig.setTypes(FooKey.class, Foo.class);
		cacheConfig.setName(CACHE_NAME);
		cacheConfig.setIndexedTypes(FooKey.class, Foo.class);

		ignite.createCache(cacheConfig);

	}

	@After
	public void afterClass() {
		Ignition.stop(true);
	}

	@Test
	public void testSqlQueryWithStringPredicate() throws SQLException {

		IgniteCache<FooKey, Foo> cache = Ignition.ignite().cache(CACHE_NAME);

		LocalDateTime da = LocalDateTime.of(2017, 1, 1, 1, 1);
		LocalDateTime db = LocalDateTime.of(2016, 1, 1, 1, 1);

		cache.put(new FooKey(1L), new Foo("a", da));
		cache.put(new FooKey(2L), new Foo("b", db));

		SqlQuery<FooKey, Foo> query = new SqlQuery<>(Foo.class, "strValue = ?");
		query.setArgs("a");

		List<Foo> foos = cache.query(query).getAll().stream().map(Cache.Entry::getValue).collect(Collectors.toList());

		assertEquals(1, foos.size());
		assertEquals("a", foos.get(0).strValue);
		assertEquals(da, foos.get(0).date);

	}

	@Test
	public void testSqlQueryWithLocalDateTimePredicate() throws SQLException {

		IgniteCache<FooKey, Foo> cache = Ignition.ignite().cache(CACHE_NAME);

		LocalDateTime da = LocalDateTime.of(2017, 1, 1, 1, 1);
		LocalDateTime db = LocalDateTime.of(2016, 1, 1, 1, 1);

		cache.put(new FooKey(1L), new Foo("a", da));
		cache.put(new FooKey(2L), new Foo("b", db));

		SqlQuery<FooKey, Foo> query = new SqlQuery<>(Foo.class, "date = ?");
		query.setArgs(da);

		List<Foo> foos = cache.query(query).getAll().stream().map(Cache.Entry::getValue).collect(Collectors.toList());

		assertEquals(1, foos.size());
		assertEquals("a", foos.get(0).strValue);
		assertEquals(da, foos.get(0).date);

	}

	static final class Foo {

		@QuerySqlField
		String strValue;

		@QuerySqlField
		LocalDateTime date;

		public Foo() {
		}

		public Foo(String strValue, LocalDateTime date) {
			this.strValue = strValue;
			this.date = date;
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
}
