package at.rocworks.gateway.logger.influx

import at.rocworks.gateway.core.data.DataPoint
import at.rocworks.gateway.core.logger.LoggerBase
import com.influxdb.client.InfluxDBClient
import com.influxdb.client.InfluxDBClientFactory
import com.influxdb.client.InfluxDBClientOptions
import com.influxdb.client.QueryApi
import com.influxdb.client.WriteApi
import com.influxdb.client.WriteOptions
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.write.Point
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.util.concurrent.Callable


class InfluxDBV2Logger(config: JsonObject) : LoggerBase(config) {
    private var enabled = false
    private val username = config.getString("Username", "")
    private val password = config.getString("Password", "")
    private val token = config.getString("Token", "")
    private val bucket = config.getString("Database", "test")
    private val org = config.getString("Org", "")

    private var client: InfluxDBClient? = null
    private var writeApi: WriteApi? = null
    private var queryApi: QueryApi? = null

    private val options = InfluxDBClientOptions.builder()
        .url(config.getString("Url", ""))
        .authenticate(username, password.toCharArray())
        .bucket(bucket)
        .org(org)
        .build()

    private fun connect() = InfluxDBClientFactory.create(options)

    override fun open(): Future<Unit> {
        val result = Promise.promise<Unit>()
        vertx.executeBlocking(Callable {
            try {
                connect().let {
                    client = it
                    val fluxQuery = """from(bucket: "$bucket") |> range(start: -1s)"""
                    it.queryApi.query(fluxQuery)

                    writeApi = it.makeWriteApi(
                        WriteOptions.builder()
                            .bufferLimit(writeParameterBlockSize)
                            .build()
                    )

                    queryApi = it.queryApi

                    logger.info("InfluxDB connected.")
                    enabled = true
                    result.complete()
                }
            } catch (e: Exception) {
                logger.severe("InfluxDB connect failed! [${e.message}]")
                enabled = false
                result.fail(e)
            }
        })
        return result.future()
    }

    override fun close(): Future<Unit> {
        val promise = Promise.promise<Unit>()
        client?.close()
        enabled = false
        promise.complete()
        return promise.future()
    }

    override fun isEnabled(): Boolean {
        return enabled
    }

    private fun influxPointOf(dp: DataPoint): Point {
        val point = Point.measurement(dp.topic.systemName) // TODO: configurable measurement name
            .addTag("tag", dp.topic.getBrowsePathOrNode().toString()) // TODO: add topicName, topicType, topicNode, ...
            .addTag("address", dp.topic.topicNode)
            .addTag("status", dp.value.statusAsString())
            .time(dp.value.sourceTime().toEpochMilli(), WritePrecision.MS)

        when (val tmp = dp.value.value) {
            is Number -> point.addField("double", tmp as Double)
            is Boolean -> point.addField("boolean", if (tmp) 1 else 0)
            is String -> point.addField("text", tmp)
            is Array<*> -> {
                point.addField("array", tmp.map { it.toString() }.toString())
            }

            else -> println("value type not supported")
        }
        return point
    }

    override fun writeExecutor() {
        val points = mutableListOf<Point>()
        pollDatapointBlock {
            points.add(influxPointOf(it))
        }
        if (points.size > 0) {
            try {
                writeApi?.writePoints(points)
                commitDatapointBlock()
                valueCounterOutput += points.size
            } catch (e: Exception) {
                logger.severe("Error writing batch [${e.message}]")
            }
        }
    }
}