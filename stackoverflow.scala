val t0 = System.nanoTime()
// General purpose library
import scala.xml._

// Spark data manipulation libraries
import org.apache.spark.rdd._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

def similarityUDF(p :(String,Map[String,Int]),q: Map[String,Int]):(String ,Double) = {
	val pkeySet = p._2.keys.toSet
	val qkeySet = q.keys.toSet
	val intersection = qkeySet.intersect(pkeySet).toList
	if(intersection.length<2){
		return (p._1, 0.0)
	}
	val crossProduct = intersection.map(x => p._2.get(x).getOrElse(0) * q.get(x).getOrElse(0)).reduce((x,y)=>x+y)
	val pNorm = Math.sqrt(p._2.values.map(x=>x*x).reduce((x, y) => x+y))	
	val qNorm = Math.sqrt(q.values.map(x=>x*x).reduce((x, y) => x+y))
	(p._1, crossProduct/(pNorm*qNorm))
}

val fileName = "Posts.xml"
val textFile = sc.textFile(fileName)
val postsXml = textFile.map(_.trim).
                    filter(!_.startsWith("<?xml version=")).
                    filter(_ != "<posts>").
                    filter(_ != "</posts>")

val postsRDD = postsXml.map { s =>
            val xml = XML.loadString(s)

            val id = (xml \ "@Id").text
	    val isQuestion = (xml \ "@PostTypeId").text
	    val parentId = (xml \ "@ParentId").text
           // val acceptAnswer = (xml \ "@AcceptedAnswerId").text
            val score = (xml \ "@Score").text.toInt
	   // val viewCount = (xml \"@ViewCount").text
	    val ownerUser = (xml \"@OwnerUserId").text
	    val tags = (xml \ "@Tags").text.replace("<","").replace(">",",").dropRight(1)
           // val title = (xml \ "@Title").text
    	   // val favoriteCount = (xml \ "@FavoriteCount").text

            Row(id,tags,isQuestion,parentId,ownerUser,score)
        }

val schemaString = "Id Tags Question ParentId OwnerUser"

val stringschema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

val schema = stringschema.add("Score", IntegerType, true)

val postsDfWithEmptyOwner = sqlContext.createDataFrame(postsRDD, schema)

val postsDf = postsDfWithEmptyOwner.filter('OwnerUser > 0)

val questionDf = postsDf.filter('Question < 2).drop("ParentId").drop("Score").drop("Question").withColumnRenamed("Id","QuestionId").withColumnRenamed("OwnerUser","QuestionOwnerUser")

val answerDf = postsDf.filter('Question >1).drop("AcceptAnswer").drop("ViewCount").drop("FavoriteCount").drop("Question").drop("Tags").withColumnRenamed("Id","AnswerId").withColumnRenamed("OwnerUser","AnswerOwnerUser")

questionDf.registerTempTable("question")

answerDf.registerTempTable("answer")

val questionTagsDf = sqlContext.sql("SELECT * from question INNER JOIN answer ON question.QuestionId = answer.ParentId")  

val questionTagDf = questionTagsDf.withColumn("Tags",explode(split('Tags,",")))

val questionordered = questionTagDf.sort('AnswerOwnerUser,'Tags)

val userTag = questionordered.groupBy('AnswerOwnerUser,'Tags).sum("Score")

val formatted = userTag.rdd.map(x=>(x(0).toString, (x(1).toString, x(2).toString.toInt))).groupByKey.mapValues(_.toMap)

val specificUser = (formatted.filter(_._1=="522444")).collect

val similarity = formatted.map(x=>similarityUDF(x,specificUser(0)._2))

val result = similarity.filter(_._2<1).sortBy(_._2, false).toDF("AnswererId","Similarity Coefficient")

result.show()

val t1 = System.nanoTime()

val duration = (t1 - t0)/1000000000

var durationFormat = duration.toString

if(duration>=60){
	durationFormat = duration/60 + " min " + duration%60 + " sec"
}

if(duration>=3600){
	durationFormat = duration/3600 + " h " + (duration%3600)/60 + " min " + (duration%3600)%60 + " sec"
}

println("Elapsed time: " + durationFormat + "s")
