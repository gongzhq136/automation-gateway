package at.rocworks.gateway.logger.neo4j

import at.rocworks.gateway.core.data.DataPoint
import at.rocworks.gateway.core.data.Topic
import at.rocworks.gateway.core.logger.LoggerBase
import at.rocworks.gateway.core.service.ServiceHandler
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.servicediscovery.Status

import java.util.concurrent.TimeUnit

import kotlin.Any
import kotlin.Boolean
import kotlin.Exception
import kotlin.Long
import kotlin.String
import kotlin.Unit

import org.neo4j.driver.*
import org.neo4j.driver.Values.parameters

import java.time.Duration
import java.time.Instant

import kotlin.concurrent.thread

class Neo4jLogger(config: JsonObject) : LoggerBase(config) {
    private val url = config.getString("Url", "bolt://localhost:7687")
    private val username = config.getString("Username", "neo4j")
    private val password = config.getString("Password", "password")

    private val driver : Driver = GraphDatabase.driver( url, AuthTokens.basic( username, password ) )

    private var session : Session? = null

    private val opcUaBulkWriteNodesQuery = """
            UNWIND ${"$"}nodes AS node
            MERGE (n:OpcUaNode {
              System : node.System,
              NodeId : node.NodeId
            }) 
            SET n += {
              Status : node.Status,
              Value : node.Value,
              DataType: node.DataType,
              ServerTime : node.ServerTime,
              SourceTime : node.SourceTime
            }  
            """.trimIndent()

    private val mqttCheckValueQuery = "MATCH (n:MqttValue) WHERE (n.System=\$record.System AND n.NodeId=\$record.NodeId) RETURN ID(n)"
    private val mqttWriteValueQuery = """
            MERGE (n:MqttValue {
              Name: ${"$"}record.Name,
              System : ${"$"}record.System,
              NodeId : ${"$"}record.NodeId
            }) 
            SET n += {
              Value : ${"$"}record.Value,
              Status : ${"$"}record.Status,
              DataType: ${"$"}record.DataType,
              ServerTime :${"$"}record.ServerTime,
              SourceTime : ${"$"}record.SourceTime
            }  
            RETURN ID(n)
            """.trimIndent()

    override fun start(startPromise: Promise<Void>) {
        super.start(startPromise)
        val session = driver.session()
        createMqttIndexes(session)
        val schemas = config.getJsonArray("Schemas", JsonArray()) ?: JsonArray()
        schemas.filterIsInstance<JsonObject>().map { systemConfig ->
            val system = systemConfig.getString("System")
            val nodeIds = systemConfig.getJsonArray("RootNodes", JsonArray(listOf("i=85"))).filterIsInstance<String>()
            fetchSchema(system, nodeIds).onComplete {
                logger.info("Write Graph [${system}] [${it.result().first}]")
                //File("${system}.json").writeText(it.result().second.encodePrettily())
                if (it.result().first) {
                    thread { writeSchemaToDb(session, system, it.result().second) }
                }
            }
        }
    }

    private fun fetchSchema(system: String, nodeIds: List<String>): Future<Pair<Boolean, JsonArray>> { // TODO: copied from GraphQLServer
        val promise = Promise.promise<Pair<Boolean, JsonArray>>()
        val serviceHandler = ServiceHandler(vertx, logger)
        val type = Topic.SystemType.Opc.name
        logger.info("Wait for service [${system}]...")
        var done = false
        serviceHandler.observeService(type, system) { record ->
            if (record.status == Status.UP && !done) {
                logger.info("Request schema [${system}] [${nodeIds.joinToString(",")}] ...")
                vertx.eventBus().request<JsonArray>(
                    "${type}/${system}/Schema",
                    JsonObject().put("NodeIds", nodeIds),
                    DeliveryOptions().setSendTimeout(60000L*10)) // TODO: configurable?
                {
                    done = true
                    logger.info("Schema response [${system}] [${it.succeeded()}] [${it.cause()?.message ?: ""}]")
                    val result = (it.result().body()?: JsonArray())
                    promise.complete(Pair(it.succeeded(), result))
                }
            }
        }
        return promise.future()
    }

    private fun createMqttIndexes(session: Session) {
        session.run("""
            CREATE INDEX IF NOT EXISTS
            FOR (n:MqttNode)
            ON (n.System, n.NodeId)
            """.trimIndent())
        session.run("""
            CREATE INDEX IF NOT EXISTS
            FOR (n:MqttValue)
            ON (n.System, n.NodeId)
            """.trimIndent())

    }

    private fun writeSchemaToDb(session: Session, system: String, schema: JsonArray) {
        try {
            logger.info("Write schema to graph database...")
            val tStart = Instant.now()
            session.run("""
                CREATE INDEX IF NOT EXISTS
                FOR (n:OpcUaNode)
                ON (n.System, n.NodeId)
                """.trimIndent())

            // Create and get system
            val systemId = session.run("MERGE (s:OpcUaSystem { DisplayName: \$DisplayName }) RETURN ID(s)",
                parameters("DisplayName", system)).single()[0]

            schema.filterIsInstance<JsonObject>().forEach { rootNode ->
                session.executeWrite { tx ->
                    // Create root node
                    val rootId = tx.run(
                        """
                        MERGE (n:OpcUaRoot {System: ${"$"}System, NodeId: ${"$"}NodeId}) 
                        SET n += {
                          NodeClass: ${"$"}NodeClass,
                          BrowseName: ${"$"}BrowseName,
                          DisplayName: ${"$"}DisplayName
                        }
                        RETURN ID(n)
                        """.trimIndent(),
                        parameters(
                            "System", system,
                            "NodeId", rootNode.getValue("NodeId"),
                            "NodeClass", rootNode.getValue("NodeClass"),
                            "BrowseName", rootNode.getValue("BrowseName"),
                            "DisplayName", rootNode.getValue("DisplayName"))
                    ).single()[0]

                    // System to root node relation
                    tx.run("""
                        MATCH (n1:OpcUaSystem) WHERE ID(n1) = ${"$"}SystemId
                        MATCH (n2:OpcUaRoot) WHERE ID(n2) = ${"$"}RootId
                        MERGE (n1)-[:HAS]->(n2)
                        """.trimIndent(),
                        parameters("SystemId", systemId, "RootId", rootId))

                    fun addNodes(parent: Value, nodes: JsonArray) {
                        val rows = mutableListOf<Map<String, Any?>>()
                        val items = nodes.filterIsInstance<JsonObject>()
                        items.forEach {
                            val node = HashMap<String, Any>()
                            node["NodeId"] = it.getString("NodeId")
                            node["NodeClass"] = it.getString("NodeClass")
                            node["BrowseName"] = it.getString("BrowseName")
                            node["BrowsePath"] = it.getString("BrowsePath")
                            node["DisplayName"] = it.getString("DisplayName")
                            node["ReferenceType"] = it.getString("ReferenceType")
                            rows.add(node)
                        }
                        val res = tx.run(
                            """
                            UNWIND ${"$"}rows AS node
                            MATCH (n1) WHERE ID(n1) = ${"$"}Parent
                            MERGE (n2:OpcUaNode {System: ${"$"}System, NodeId: node.NodeId})
                            SET n2 += {
                              NodeClass : node.NodeClass,
                              BrowseName : node.BrowseName,
                              BrowsePath : node.BrowsePath,
                              DisplayName : node.DisplayName
                            }
                            MERGE (n1)-[:HAS {ReferenceType: node.ReferenceType}]->(n2)
                            RETURN ID(n2)
                            """.trimIndent(),
                            parameters(
                                "Parent", parent,
                                "System", system,
                                "rows", rows
                            )
                        )
                        //logger.info("Wrote ${rows.size} nodes.")
                        items.zip(res.list()).forEach {
                            val nextNodes = it.first.getJsonArray("Nodes")
                            if (nextNodes != null && !nextNodes.isEmpty) {
                                addNodes(it.second[0], nextNodes)
                            }
                        }
                    }
                    addNodes(rootId, rootNode.getJsonArray("Nodes", JsonArray()))
                }
            }
            val duration = Duration.between(tStart, Instant.now())
            val seconds = duration.seconds + duration.nano/1_000_000_000.0
            logger.warning("Writing schema to graph database took [${seconds}]s")

        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    override fun open(): Future<Unit> {
        logger.info("Open $username $password")
        val promise = Promise.promise<Unit>()
        try {
            this.session = driver.session()
            promise.complete()
        } catch (e: Exception) {
            promise.fail(e.message)
        }
        return promise.future()
    }

    override fun close(): Future<Unit> {
        val promise = Promise.promise<Unit>()
        driver.closeAsync().whenComplete { _, _ ->
            promise.complete()
        }
        return promise.future()
    }

    override fun writeExecutor() {
        val opcUaNodes = mutableListOf<Map<String, Any?>>()
        var point: DataPoint? = writeValueQueue.poll(10, TimeUnit.MILLISECONDS)
        while (point != null && opcUaNodes.size < writeParameterBlockSize) {
            logger.finest { "Write topic ${point?.topic}" }
            when (val systemType = point.topic.systemType) {
                Topic.SystemType.Opc -> {
                    addOpcUaNode(opcUaNodes, point)
                    valueCounterOutput ++
                }
                Topic.SystemType.Mqtt -> {
                    writeMqttValue(point)
                    valueCounterOutput ++
                }
                else -> { logger.warning("$systemType not supported!")}
            }
            point = writeValueQueue.poll()
        }
        writeOpcUaNodes(opcUaNodes)
    }

    private fun addOpcUaNode(opcUaRecords: MutableList<Map<String, Any?>>, point: DataPoint) {
        opcUaRecords.add(
            mapOf(
                "System" to point.topic.systemName,
                "NodeId" to point.topic.node,
                "Status" to point.value.statusAsString(),
                "Value" to point.value.valueAsObject(),
                "DataType" to point.value.dataTypeName(),
                "ServerTime" to point.value.serverTimeAsISO(),
                "SourceTime" to point.value.sourceTimeAsISO()
            )
        )
    }

    private fun writeOpcUaNodes(opcUaRecords: MutableList<Map<String, Any?>>) {
        if (opcUaRecords.size > 0) { // Bulk Write Operation
            session?.executeWrite { tx ->
                tx.run(opcUaBulkWriteNodesQuery, parameters("nodes", opcUaRecords))
            }
        }
    }

    private fun writeMqttValue(point: DataPoint) {
        val path = point.topic.node.split("/")
        val name = path.last()

        val record = mapOf(
            "Name" to name,
            "System" to point.topic.systemName,
            "NodeId" to point.topic.node,
            "Status" to point.value.statusAsString(),
            "Value" to point.value.valueAsObject(),
            "DataType" to point.value.dataTypeName(),
            "ServerTime" to point.value.serverTimeAsISO(),
            "SourceTime" to point.value.sourceTimeAsISO())

        session?.executeWrite { tx ->
            val isNewValue = tx.run(mqttCheckValueQuery, parameters("record", record)).list().size == 0
            val mqttValueId = tx.run(mqttWriteValueQuery, parameters("record", record)).single()[0]
            if (isNewValue) writeMqttValuePath(tx, point, path, mqttValueId)
        }
    }

    private fun writeMqttValuePath(
        tx: TransactionContext,
        point: DataPoint,
        path: List<String>,
        mqttValueId: Value
    ) {
        val connectQuery =
            "MATCH (n1) WHERE ID(n1) = \$parentId \n" +
                    "MATCH (n2) WHERE ID(n2) = \$folderId \n" +
                    "MERGE (n1)-[:HAS]->(n2)"

        val (parentId, _) = (listOf(point.topic.systemType.toString(), point.topic.systemName) + path)
            .dropLast(1) // remove the MqttValue
            .fold(Pair(Values.NULL, "")) { pair, name ->
                val path = pair.second + "/" + name
                val folderId = tx.run(
                    "MERGE (n:MqttNode { System: \$System, Path: \$Path, Name: \$Name }) RETURN ID(n)",
                    parameters("System", point.topic.systemName, "Path", path, "Name", name)
                ).single()[0]

                if (!pair.first.isNull) {
                    val parentId = pair.first
                    tx.run(
                        connectQuery,
                        parameters("parentId", parentId, "folderId", folderId)
                    )
                }
                Pair(folderId, path)
            }
        tx.run(
            connectQuery,
            parameters("parentId", parentId, "folderId", mqttValueId)
        )
    }

    override fun queryExecutor(
        system: String,
        nodeId: String,
        fromTimeMS: Long,
        toTimeMS: Long,
        result: (Boolean, List<List<Any>>?) -> Unit
    ) {
        TODO("Not yet implemented")
    }

    override fun getComponentGroup(): ComponentGroup {
        return ComponentGroup.Logger
    }
}