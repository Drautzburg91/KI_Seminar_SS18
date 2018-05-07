package poker

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.model.RandomForestModel


object PokerConsole {
  var loadedModel: RandomForestModel = null
  var sc: SparkContext = null

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Poker Prediction Console")
    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    startPokerPredictionConsole()
  }

  private def startPokerPredictionConsole(): Unit ={

    var quit: Boolean = false;
    var help: Boolean = false;

    println("-------------------------------------------------------------------")
    println("-------------------------------------------------------------------")
    println("-------------------------------------------------------------------")
    println("------          Poker Prediction Console Interface           ------")
    println("------          [SPARK MLlib Random Forest Model]            ------")
    println("-------------------------------------------------------------------")
    println("------              HTWG MSI SEMINAR SS 2018                 ------")
    println("-------------------------------------------------------------------")
    println("-------------------------------------------------------------------")
    while(quit != true){
      println("-------------------------------------------------------------------")
      println("Enter Classifier Model Path [\\PATH\\PokerPredictionModel_DateTime.model]: ")
      val inputString = scala.io.StdIn.readLine()

      if(loadClassifierModelFromPath(inputString)){
        println("-------------------------------------------------------------------")
        println("Classifier Loaded Successful!")
        quit = true
      }else {
        println("-------------------------------------------------------------------")
        println("Classifier couldn't be loaded")
      }

    }
    println("-------------------------------------------------------------------")
    quit = false
    println("You can type in a Poker Hand Set, coded like the following sample and let the Classifier predict your result")
    println("STRAIGHT FLUSH Example Input:")
    println("Hearts,Ace;Hearts,Two;Hearts,Three;Hearts,Four;Hearts,Five")
    println("-----------------------------------")
    println("Enter \"-h\" to get description of poker hand input format")
    println("Enter \"-q\" to exit")
    println("-------------------------------------------------------------------")


    while(quit != true) {
      help = false
      println("---------------------------------")
      println(">>Enter Poker Data Hand to predict: ")
      val inputString = scala.io.StdIn.readLine()

      if (inputString == "-q") {
        quit = true
      }

      if (inputString == "-h") {
        printInputOption()
        help = true
      }

      if (quit != true && help != true) {
      var inputcheck: Boolean = false
      val pokerhandTransform = new PokerHandTransform()
      val inputString2 = inputString.replace(" ","")
      inputcheck  = pokerhandTransform.checkInput(inputString2)

        if(inputcheck == true) {
          val labledPoint = pokerhandTransform.getPokerHandAsRddLabeldPoint()
          val test = labledPoint.features
          println(test)
          val predictionResult =loadedModel.predict(labledPoint.features)           //Random Forest Prediction
          val nTrees = loadedModel.trees.size

          //Get Prediction Per Tree
          var predictionPerTree = new Array[Double](nTrees)
          var highestPrediction = 0.0
          var treeWithHighestPrediction = 0
          var i = 0
          loadedModel.trees.foreach(t =>
          {
            predictionPerTree(i) = t.predict(labledPoint.features)
            if(highestPrediction < predictionPerTree(i))
            {
              highestPrediction = predictionPerTree(i)
              treeWithHighestPrediction = i
            }
            i +=1
          }
          )
          val predictionResultString = pokerhandTransform.getPredictionString(predictionResult)
          println(">>>RandomForest Prediction: "+predictionResult.toInt +" - "+predictionResultString)
          val highestPredictionResString = pokerhandTransform.getPredictionString(highestPrediction)
          println(">>>Highest Prediction in RandomForest: "+highestPrediction.toInt +" - "+highestPredictionResString)
        }
        else{
          println("!Sorry, Poker Hand Input not valid and couldn't be processed")
          println("Enter \"-h\" to get description of poker hand input format")
          println("---------------------------------")
        }
      }
    }

    println("-------------------------------------------------------------------")
    println("Bye - We hope you had some fun!")
    println("-------------------------------------------------------------------")
    println("-------------------------------------------------------------------")
  }

  def printInputOption(): Unit = {
    println("-------------------------------------------------------------------")
    println("HELP - Poker Hand Input")
    println("-------------------------------------------------------------------")
    println("Description of Poker Hand Input Format:")
    println(" Use following format to create a valid Poker Hand Input String.")
    println(" A poker hand consists of 5 cards, which are discribed by a Suit and a Rank.")
    println(" Each Card is described by pair of Suit and a Rank: [C1_Suit,C1_Rank]")
    println(" Each card (represented by a Suit, Rank pair) is seperated with the char \";\" \n")
    println(" Valid Poker Hand Format (5 Cards - pairs of Suit,Rank): ")
    println("   C1_Suit,C1_Rank;C2_Suit,C2_Rank;C3_Suit,C3_Rank;C4_Suit,C4_Rank;C5_Suit,C5_Rank \n")
    println("Example for Straight Flush with Hearts (Suit): ")
    println("   English: Hearts,Ace; Hearts,Two; Hearts,Three; Hearts,Four; Hearts,Five")
    println("   German:  Herz,Ass; Herz,Zwei; Herz,Drei; Herz,Vier; Herz,Fünf")
    println("----------------------------")
    println("Suit Options: ")
    println("  English: Hearts, Spades, Diamonds, Clubs")
    println("  German:  Herz, Pik, Karo, Kreuz \n")
    println("Rank Options: ")
    println("  English: Ace, Two, Three, Four, Five, Six, Seven, Eight, Nine, Ten, Jack, Queen, King")
    println("  German: Ass, Zwei, Drei, Vier, Fünf, Sechs, Sieben, Acht, Neun, Zehn, Bube, Dame, König \n")
    println("Value Prediction Definition:")
    println("  0: Nothing in hand   - not a recognized poker hand")
    println("  1: One pair          - one pair of equal ranks within five cards")
    println("  3: Two pairs         - two pairs of equal ranks within five cards")
    println("  3: Three of a kind   - three equal ranks within five cards")
    println("  4: Straight          - five cards, sequentially ranked with no gaps")
    println("  5: Flush             - five cards with the same suit")
    println("  6: Full House        - pair + different rank three of a kind")
    println("  7: Four of a kind    - four equal ranks within five cards")
    println("  8: Straight Flush    - straight + flush")
    println("  9: Royal flush       - {Ace, King, Queen, Jack, Ten} + flush")
    println("----------------------------")
    println("-------------------------------------------------------------------")
  }

  def loadClassifierModelFromPath(classifierModelPath: String): Boolean ={
    this.loadedModel = RandomForestModel.load(this.sc,classifierModelPath)
      if(loadedModel != null){
        return true
      }else false
  }

}
