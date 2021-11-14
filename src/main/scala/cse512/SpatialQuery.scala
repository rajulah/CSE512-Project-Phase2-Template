package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
 def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    if(Option(queryRectangle).getOrElse("").isEmpty|| Option(pointString).getOrElse("").isEmpty)
      return false
    var rectangleArray = queryRectangle.split(",")
    var rectangleX1     = rectangleArray(0).trim.toDouble
    var rectangleY1     = rectangleArray(1).trim.toDouble
    var rectangleX2     = rectangleArray(2).trim.toDouble
    var rectangleY2     = rectangleArray(3).trim.toDouble

    var arrayOfPoint = pointString.split(",")
    var pointX = arrayOfPoint(0).trim.toDouble
    var pointY = arrayOfPoint(1).trim.toDouble

    if (pointX >= rectangleX1 && pointX <= rectangleX2 && pointY >= rectangleY1 && pointY <= rectangleY2)
      return true
    else if (pointX <= rectangleX1 && pointX >= rectangleX2 && pointY <= rectangleY1 && pointY >= rectangleY2)
      return true
    else
      return false
  }

def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {

    var point1:Array[String] = new Array[String](2)
    point1 = pointString1.split(",")
    val p1_x1 = point1(0).trim().toDouble
    val p1_y1 = point1(1).trim().toDouble

    var point2:Array[String] = new Array[String](2)
    point2 = pointString2.split(",")
    val p2_x2 = point2(0).trim().toDouble
    val p2_y2 = point2(1).trim().toDouble

    val distEuclidean = math.pow(math.pow((p1_x1 - p2_x2), 2) + math.pow((p1_y1 - p2_y2), 2),0.5)

    if (distEuclidean > distance)
      return false
    else
      return true
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
