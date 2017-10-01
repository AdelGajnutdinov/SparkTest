import org.apache.spark.{SparkConf, SparkContext}

object Numbers {
  def main(args: Array[String]): Unit = {
    val inputFile = "C:/dataForScalaProjects/numbers.txt"
    val conf = new SparkConf().setAppName("wordCount")
    conf.setMaster("local[2]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    val input = sc.textFile(inputFile) //RDD[String]

    //    Calculate the sum of the numbers in each row of file
    val numbers = input.map(string => string.split(" ")) //RDD[Array[String]]
    val numbersArray = numbers.map(s => s.map(a => a.toInt)) //RDD[Array[Integer]]
    val answer = numbersArray.map(d => d.reduce((a, b) => a + b))
    answer.foreach(println)

    //    Calculate the sum of the numbers i in each row of file, which satisfy i%5=0
    val filteredNumbersArray = numbersArray.map(s => s.filter(a => a % 5 == 2)) //RDD[Array[FilteredInteger]]
    val answer1 = filteredNumbersArray.map(d => d.sum)
    answer1.foreach(println)
  }
}
