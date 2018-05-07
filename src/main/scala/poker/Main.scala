package poker

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.{RandomForest}
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.storage.StorageLevel

import java.io._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter


object PokerPrediction {
  def main(args: Array[String]): Unit = {

    //Configure & Run Spark
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Poker Prediction")
    val sc = new SparkContext(conf)

    val dateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd_MM_yyyy__HH_mm"))
    val startTime = System.nanoTime()

    // Load and parse the data files (1 million in Training Data & 20 000 in Testdata)
    //val trainingData = MLUtils.loadLibSVMFile(sc, "/Users/paul/Seminar_KI_SS18/data/poker.t")
    //val testData     = MLUtils.loadLibSVMFile(sc, "/Users/paul/Seminar_KI_SS18/data/poker")
    var trainingData = MLUtils.loadLibSVMFile(sc, "C:\\Workspace\\Seminar_KI_SS18\\data\\poker.t") //CHECK
    val testData = MLUtils.loadLibSVMFile(sc, "C:\\Workspace\\Seminar_KI_SS18\\data\\poker")
    trainingData.persist(StorageLevel.MEMORY_AND_DISK_SER)                                         //Use Disk Memory, if MainMemory is not enough

    val numLinesTraining = trainingData.count()
    println("Training Lines: " + numLinesTraining)

    val numLinesTest = testData.count()
    println("Test Lines: " + numLinesTest)


    // Train a RandomForest model
    //Model Parameter Configuration
    val numClasses = 10                        //10 Features in each record of DataSet (Describing one PokerHand)
    val categoricalFeaturesInfo = Map[Int, Int](0 -> 5, 1 -> 14, 2 -> 5, 3 -> 14, 4 -> 5, 5 -> 14, 6 -> 5, 7 -> 14, 8 -> 5, 9 -> 14)
    val numTrees = 1
    val featureSubsetStrategy = "all"
    val impurity = "gini"                     // possibilities are "gini" or "entropy"
    val maxDepth = 25                         // min 10 is needed in our case
    val maxBins = 1024                        // 2^10 (10 Features to compute Information Gain)


    //Train the model
    val model = RandomForest.trainClassifier(trainingData,
                                              numClasses,
                                              categoricalFeaturesInfo,
                                              numTrees,
                                              featureSubsetStrategy,
                                              impurity,
                                              maxDepth,
                                              maxBins,
                                              seed = 12345)


    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction, point.features)
    }


    //Print out results (prediction) with label > 4
    labelAndPreds.foreach(p => if (p._1 > 4 && (p._1 == p._2)) {
      println(">> Expected: " + p._1 + " - Prediction: " + p._2 + " -- " + p._3)
    })

    //Calculate Error Rate for Prediction
    val testErrDataSet = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    val testErrDataSetwithTol = labelAndPreds.filter(r => (r._1 - r._2) > 1).count.toDouble / testData.count()

    println(">>Data Set Test Error = " + testErrDataSet)
    println(">>Data Set Test Error (Delta 1 Tolerance)= " + testErrDataSetwithTol)


    // Save model
    model.save(sc, "C:\\SparkPokerPrediction\\models\\PokerPrediction_"+dateTime+".model")                       //CHECK
    //model.save(sc, "/Users/paul/Desktop/SparkPokerPrediction/models/PokerPrediction_" + dateTime + ".model")


    //Log Information
    val logFile = new PrintWriter(new File("C:\\SparkPokerPrediction\\PokerPrediction_" + dateTime + "Uhr.txt")) //CHECK
    //val logFile = new PrintWriter(new File("/Users/paul/Desktop/SparkPokerPrediction/"+dateTime+"Uhr.txt" ))

    logFile.write("--------------------------------------------- \n")
    logFile.write("Poker Prediction - RandomForest Approach \n")
    logFile.write("---------------------------------------------\n")
    logFile.write("--------------------------------------------- \n")
    logFile.write("Dataset information")
    logFile.write("\n Training Lines: " + numLinesTraining)
    logFile.write("\n Test Lines: " + numLinesTest)
    logFile.write("\n--------------------------------------------- \n")
    logFile.write("Some Data Samples \n")
    logFile.write("\n--------------------------------------------- \n")
    logFile.write("Model Configuration")
    logFile.write("\n numClasses: " + numClasses)
    logFile.write("\n numTrees: " + numTrees)
    logFile.write("\n featureSubsetStrategy: " + featureSubsetStrategy)
    logFile.write("\n impurity: " + impurity)
    logFile.write("\n maxDepth: " + maxDepth)
    logFile.write("\n maxBins: " + maxBins)
    logFile.write("\n--------------------------------------------- \n")
    logFile.write("Result Overview")
    logFile.write("\n >>Data Set Test Error = " + testErrDataSet)
    logFile.write("\n >>Data Set Test Error (Delta 1 Tolerance)= " + testErrDataSetwithTol)
    logFile.write("\n--------------------------------------------- \n")
    logFile.write("Decision Tree(s) \n")
    logFile.write(model.toDebugString)
    logFile.write("\n ---------------------------------------------")

    //Calculate Training Time
    val endTime = System.nanoTime()
    var trainTime = endTime - startTime
    trainTime /= 10000000000L //seconds
    logFile.write("\n Trainingtime: " + trainTime + " seconds \n")
    trainTime /= 60 //minutes
    logFile.write("\n Trainingtime: " + trainTime + " minutes \n")
    trainTime /= 60 //hours
    logFile.write("\n Trainingtime: " + trainTime + " hours \n")

    println("Training done!!!!")
    logFile.close
    sc.stop();

  }
}

object TestLoadPokerPredictionLoadModel {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Poker Prediction Loaded")

    val sc = new SparkContext(conf)

    //Load Model
    val loadModel = RandomForestModel.load(sc, "/Users/paul/Desktop/SparkPokerPrediction/models" + ".model")
    //val loadModel = RandomForestModel.load(sc, "C:\\SparkPokerPrediction\\models\\PokerPredictionModel_01_05_2018__00_10Uhr.model")  //CHECK

  }
}

