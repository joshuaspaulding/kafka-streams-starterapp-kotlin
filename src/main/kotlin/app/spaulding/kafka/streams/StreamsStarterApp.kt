package app.spaulding.kafka.streams

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig

/**
 * Demo Kakfa Streams app. Foundation for the other Stream classes.
 *
 * @author joshua.spaulding
 */
object StreamsStarterApp {

    @JvmStatic
    fun main(args: Array<String>) {

        val config = Properties()
        config[StreamsConfig.APPLICATION_ID_CONFIG] = "streams-starter-app"
        config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        config[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
        config[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

        val builder = StreamsBuilder()

        val kStream = builder.stream<String, String>("streams-input")
        // do stuff
        kStream.to("streams-output")

        val streams = KafkaStreams(builder.build(), config)
        streams.cleanUp() // only do this in dev - not in prod
        streams.start()

        // print the topology
        println(streams.localThreadsMetadata().toString())

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(Thread(Runnable { streams.close() }))

    }

}
