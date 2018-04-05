import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.mockito.Mockito._
import scala.collection.JavaConversions._
import kafka.producer.{DefaultPartitioner => Legacy}

// See https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java
object Main extends App {
  val key = "801000000293408"
  val numPartitions = 100

  // Results
  println("")
  println("----------------------")
  println("Legacy Partioner (Deprecated)")
  println("key, positiveHash, 2, 3, 100, 99")
  println(s"""{${key}, ${Utils.abs(key.hashCode)}, ${legacy(key, 2)}, ${legacy(key, 3)}, ${legacy(key, 100)}, ${legacy(key, 999)}},""")
  println("")
  println("New Default Partioner (Murmur2)")
  println("key, positiveHash, 2, 3, 100, 99")
  println(s"""{${key}, ${Utils.toPositive(Utils.murmur2(key.getBytes))}, ${partition(key.getBytes, 2)}, ${partition(key.getBytes, 3)}, ${partition(key.getBytes, 100)}, ${partition(key.getBytes, 999)}},""")
  println("----------------------")
  println("")

  def legacy(keyBytes: Any, partitions: Int) = {
    new Legacy().partition(keyBytes, partitions)
  }

  def partition(keyBytes: Array[Byte], partitions: Int) = {
    val p = new DefaultPartitioner()
    val cluster = mock(classOf[Cluster])
    val l: java.util.List[PartitionInfo] = List.fill[PartitionInfo](partitions)(null)
    when(cluster.partitionsForTopic("topic")).thenReturn(l);
    p.partition("topic", null, keyBytes, null, null, cluster)
  }
}
