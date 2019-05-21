package com.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheQueryReadEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.cache.query.CacheQueryType;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.junit.jupiter.api.Test;

public class QueryEventTest {

	@Test
	public void test() {

		try (Ignite ignite = Ignition.start()) {
			List<Event> events = new ArrayList<>();
			ignite.events().enableLocal(EventType.EVT_CACHE_QUERY_OBJECT_READ);
			ignite.events().localListen(events::add, EventType.EVT_CACHE_QUERY_OBJECT_READ);

			CacheConfiguration<FooKey, Foo> cacheCfg = new CacheConfiguration<>("foo-cache");
			cacheCfg.setTypes(FooKey.class, Foo.class);
			QueryEntity qe = new QueryEntity(FooKey.class, Foo.class);
			cacheCfg.setQueryEntities(Collections.singletonList(qe));

			IgniteCache<FooKey, Foo> cache = ignite.createCache(cacheCfg);
			cache.put(new FooKey("a"), new Foo("A", 1));
			cache.put(new FooKey("b"), new Foo("B", 2));
			cache.put(new FooKey("c"), new Foo("C", 3));
			cache.put(new FooKey("d"), new Foo("D", 4));

			SqlFieldsQuery query = new SqlFieldsQuery("select count(*) from " + QueryUtils.typeName(Foo.class));
			try (FieldsQueryCursor<List<?>> cursor = cache.query(query)) {
				List<List<?>> res = cursor.getAll();

				assertEquals(1, res.size(), "ResultSet must contain only one row");
				assertEquals(1, res.get(0).size(), "ResultSer row must contain only one column");
				assertEquals(4L, res.get(0).get(0), "Count must be 4");

				assertEquals(1, events.size(), "Only one event must have been captured");
				assertTrue(events.get(0) instanceof CacheQueryReadEvent, "Captured event must be an instance of CacheQueryReadEvent");

				CacheQueryReadEvent<?, ?> e = (CacheQueryReadEvent<?, ?>) events.get(0);
				assertEquals(CacheQueryType.SQL_FIELDS.name(), e.queryType(), "Captured event type must be SQL_FIELDS");

			}
		}
	}
}
