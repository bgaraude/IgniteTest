package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.math.BigDecimal;
import java.sql.SQLException;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BigDecimalTest {

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
	public void testBigDecimal() throws SQLException {

		IgniteCache<FooKey, Foo> cache = Ignition.ignite().cache(CACHE_NAME);


		cache.put(new FooKey(1L), new Foo("a", new BigDecimal("1.234567E+28")));


		Foo foo = cache.query(new ScanQuery<FooKey, Foo>()).getAll().stream().map(Cache.Entry::getValue).findFirst().orElse(null);

		assertNotNull(foo);
		assertEquals("a", foo.strValue);
		assertEquals(new BigDecimal("1.234567E+28"), foo.value);

	}


	static final class Foo {

		@QuerySqlField
		String strValue;

		@QuerySqlField
		BigDecimal value;

		public Foo() {
		}

		public Foo(String strValue, BigDecimal value) {
			this.strValue = strValue;
			this.value = value;
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
