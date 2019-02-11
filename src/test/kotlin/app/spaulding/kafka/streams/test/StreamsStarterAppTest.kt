package app.spaulding.kafka.streams.test

import java.util.Properties
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.test.OutputVerifier
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test

class StreamsStarterAppTest {

    private var testDriver: TopologyTestDriver? = null
    private val recordFactory = ConsumerRecordFactory(
            StringSerializer(), StringSerializer()
    )

    @Before
    fun setUp() {

        val config = Properties()
        config[StreamsConfig.APPLICATION_ID_CONFIG] =  "streams-starter-app"
        config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] =  "dummy:1234"
        config[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        config[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

        val builder = StreamsBuilder()

        val kStream = builder.stream<String, String>("streams-input")
        // do stuff
        kStream.to("streams-output")

        testDriver = TopologyTestDriver(builder.build(), config)
    }

    @After
    fun tearDown() {
        testDriver!!.close()
    }

    @Test
    fun shouldFlushStoreForFirstInput() {
        testDriver!!.pipeInput(recordFactory.create("streams-input",
                null, "hello"))
        OutputVerifier.compareKeyValue(testDriver!!.readOutput("streams-output",
                StringDeserializer(),
                StringDeserializer()), null,
                "hello")
        Assert.assertNull(testDriver!!.readOutput("streams-output"))
    }
}