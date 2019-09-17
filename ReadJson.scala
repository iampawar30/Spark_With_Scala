
import org.apache.spark
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark
import scala.reflect.ClassTag
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession

/** A raw stackoverflow posting, either a question or an answer */


/** The main class */
object ReadJson {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Test")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val ssc = SparkSession
      .builder()
      .appName("Read JSON File to DataSet")
      .master("local[2]")
      .getOrCreate();

    //val df = ssc.read.json ("json_datafile")
    //val lines= sc.textFile("/home/gslab/Spark/Json/biz.json")
    //val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")

    val jsondata1= ssc.read.json("/home/gslab/Spark/Json/biz.json")
    //val jsondata2 = ssc.read.json(sc.wholeTextFiles("/home/gslab/Spark/Json/biz.json").values)
    jsondata1.toDF().show()
    jsondata1.select("address").where("is_open==1").toDF().show()
    //jsondata2.show()

    jsondata1.createOrReplaceTempView("biz")
    jsondata1.sqlContext.sql("select address from biz where is_open==1").show()
    jsondata1.sqlContext.sql("select count(*) from biz").show()
    jsondata1.sqlContext.sql("select city,count(1) as bi from biz where group by city order by bi DESC").show()
    //jsondata1.write.json("/home/gslab/Spark/Json/output")
  }
}

