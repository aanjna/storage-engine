package com.storagengine.moniepoint;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.junit.jupiter.api.*;

import java.io.File;
import java.util.*;

@SpringBootTest
class MoniepointApplicationTests {

	private static final String DIR = "testdata";
//	private StorageEngine store;
	@Autowired
	KeyValueStore store;
	FileWriteAheadLog wal;

	@BeforeEach
	void setup() throws Exception {
		wal = new FileWriteAheadLog("test-wal.log");
		store = new StorageEngine(wal);
	}

	@Test
	void testPutAndRead() throws Exception {
		store.put("a", "hello".getBytes());
		Optional<byte[]> val = store.read("a");

		Assertions.assertTrue(val.isPresent());
		Assertions.assertEquals("hello", new String(val.get()));
	}

	@Test
	void testBatchPut() throws Exception {
		Map<String, byte[]> map = new HashMap<>();
		map.put("a", "1".getBytes());
		map.put("b", "2".getBytes());
		map.put("c", "3".getBytes());

		store.batchPut(map);

		Assertions.assertEquals("2", new String(store.read("b").orElseThrow()));
	}

	@Test
	void testRangeRead() throws Exception {
		store.put("a", "A".getBytes());
		store.put("b", "B".getBytes());
		store.put("c", "C".getBytes());

		var range = store.readKeyRange("a", "b");

		Assertions.assertEquals(2, range.size());
		Assertions.assertTrue(range.containsKey("a"));
		Assertions.assertTrue(range.containsKey("b"));
	}

	@Test
	void testDelete() throws Exception {
		store.put("x", "test".getBytes());
		store.delete("x");

		Assertions.assertTrue(store.read("x").isEmpty());
	}

	@Test
	void testRecovery() throws Exception {
		store.put("k1", "hello".getBytes());
		store.put("x", "100".getBytes());
		store.put("y", "200".getBytes());

		// Reload engine -> should replay WAL
		wal = new FileWriteAheadLog("test-wal.log");
		store = new StorageEngine(wal);

		Assertions.assertEquals("100", new String(store.read("x").get()));
		Assertions.assertEquals("200", new String(store.read("y").get()));

		Assertions.assertEquals("hello", new String(store.read("k1").orElseThrow()));
	}
}
