import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) {
    val inputFile = "C:\\Users\\kbipi\\IdeaProjects\\WordCountExample\\src\\main\\resources\\input\\input.txt"
    val outputFile = "C:\\Users\\kbipi\\IdeaProjects\\WordCountExample\\src\\main\\resources\\output"
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("Word Count Example")
      .getOrCreate()
    import sparkSession.implicits._
    // Load our input data.
    val input = sparkSession.read.text(inputFile).as[String]
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val wordCounts = words.groupByKey(_.toLowerCase())
    // Save the word count back out to a text file, causing evaluation.
    println("Word Count—->" + wordCounts.count().show())
    wordCounts.count().toDF().rdd.coalesce(1).saveAsTextFile(outputFile)
  }
}