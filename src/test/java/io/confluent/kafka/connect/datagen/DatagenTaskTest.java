package io.confluent.kafka.connect.datagen;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatagenTaskTest {

	DatagenTask task = new DatagenTask();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public final void testStart() {
		@SuppressWarnings("unchecked")
		Map<String, String> props = mock(Map.class);
		
		when(props.get("kafka.topic")).thenReturn("site");
		when(props.containsKey("kafka.topic")).thenReturn(true);
		
		task.start(props);
	}
	
	@Test
	public final void testStop() {
		return;
	}

	@Test
	public final void testVersion() {
		return;
	}

	@Test
	public final void testPoll() {
		return;
	}

} // DatagenTaskTest
