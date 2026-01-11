import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger} // 引入日志控制

object WordCount {

  def main(args: Array[String]): Unit = {
    // 0. 设置日志级别，避免控制台被一堆红色的 INFO 信息刷屏，只看结果
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 1. 既然我们在IDEA测试，直接硬编码路径，省得去配置 args 参数了
    // 注意：Windows路径要用 / 或者 \\
    val inputPath = "D:/test.txt"   // 请确保 D盘有这个文件
    val outputPath = "D:/output_result" // 请确保 D盘没有这个文件夹，Spark会自动创建

    // 2. 创建Spark配置
    // setMaster("local[*]") 代表利用本地所有CPU核心运行
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      // 3. 读取文件
      val textFile = sc.textFile(inputPath)

      // 4. 核心统计逻辑
      val wordCount = textFile
        .flatMap(line => line.split(" ")) // 按空格切分
        .map(word => (word, 1))           // 变成 (单词, 1)
        .reduceByKey(_ + _)               // 累加

      // 5. 打印到控制台看看
      println("===== 统计结果如下 =====")
      wordCount.foreach(println)
      println("======================")

      // 6. 保存到文件 (这一步在Windows上如果没有winutils.exe可能会报错，但控制台打印成功就行)
      // wordCount.saveAsTextFile(outputPath)

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      // 7. 停止Spark
      sc.stop()
    }
  }
}