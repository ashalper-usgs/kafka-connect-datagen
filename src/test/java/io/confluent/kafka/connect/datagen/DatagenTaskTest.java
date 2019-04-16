package io.confluent.kafka.connect.datagen;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatagenTaskTest {

	static DatagenTask task = new DatagenTask();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		@SuppressWarnings("unchecked")
		Map<String, String> props = mock(Map.class);
		
		when(props.get("kafka.topic")).thenReturn("site");
		when(props.containsKey("kafka.topic")).thenReturn(true);
		
		task.start(props);
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public final void testPoll() {
		try {
			task.poll();
		} catch (InterruptedException e) {
			fail(e.getMessage());
		}
	}
	
	@Test
	public final void testStop() {
		return;
	}

	@Test
	public final void testVersion() {
		return;
	}

} // DatagenTaskTest
