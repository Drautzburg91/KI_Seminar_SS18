package poker

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

class PokerHandTransform() {

  var testString = "Hearts,Ace;Hearts,Two;Hearts,Three;Hearts,Four;Hearts,Five"
  val cardRddArray = new Array[Double](10)
  var i: Int = 0

  def checkInput(cardString: String): Boolean = {
    val cardArray = cardString.split(";")
    if (checkOnDuplicates(cardArray)) {
      cardArray.foreach(cardTupel => {
        val cardInfo = cardTupel.split(",")
        if (cardInfo.size == 2) {
          val cardSuit = getSuitDoubleValue(cardInfo(0))
          val cardRank = getRankDoubleValue(cardInfo(1))

          if ((cardSuit + cardRank) <= 1.0) //No Match in Transfer to LabledPoint
          {
            println("!Invalid Poker Hand Input - \" "+cardTupel+" \" not a valid Card Pair ")
            return false
          }
          cardRddArray(i) = cardSuit
          cardRddArray(i + 1) = cardRank
          i += 2
        }else{
          println("!Invalid Poker Hand Input - \" "+cardTupel+" \" not a valid Card Pair ")
          return false
        }
      })
      if (i != 10) {
        return false
      }
    }
    else {
      return false
    }
    return true
  }


  def getPokerHandAsRddLabeldPoint(): LabeledPoint = {
    val pokerHandFeatureVector: Vector = Vectors.dense(cardRddArray(0), cardRddArray(1), cardRddArray(2), cardRddArray(3), cardRddArray(4), cardRddArray(5), cardRddArray(6), cardRddArray(7), cardRddArray(8), cardRddArray(9))
    val pokerHandLabledPoint = LabeledPoint(0.0, pokerHandFeatureVector)

    //println("Features: " + pokerHandFeatureVector)
    return pokerHandLabledPoint
  }


  def getPredictionString(prediction: Double): String = prediction match {
    case 0.0 => "Nothing in hand"
    case 1.0 => "One Pair"
    case 2.0 => "Two Pairs"
    case 3.0 => "Three of a kind"
    case 4.0 => "Straight"
    case 5.0 => "Flush"
    case 6.0 => "Full House"
    case 7.0 => "Four of a kind"
    case 8.0 => "Straight Flush"
    case 9.0 => "Royal Flush"
  }

  private def getSuitDoubleValue(string: String): Double = string match {
    //English
    case "Hearts" => 1.0
    case "Spades" => 2.0
    case "Diamonds" => 3.0
    case "Clubs" => 4.0
    //German
    case "Herz" => 1.0
    case "Pik" => 2.0
    case "Karo" => 3.0
    case "Kreuz" => 4.0
    case _ => 0.0
  }

  private def getRankDoubleValue(string: String): Double = string match {
    //English
    case "Ace" => 1.0
    case "Two" => 2.0
    case "Three" => 3.0
    case "Four" => 4.0
    case "Five" => 5.0
    case "Six" => 6.0
    case "Seven" => 7.0
    case "Eight" => 8.0
    case "Nine" => 9.0
    case "Ten" => 10.0
    case "Jack" => 11.0
    case "Queen" => 12.0
    case "King" => 13.0
    //German
    case "Ass" => 1.0
    case "Zwei" => 2.0
    case "Drei" => 3.0
    case "Vier" => 4.0
    case "Fünf" => 5.0
    case "Sechs" => 6.0
    case "Sieben" => 7.0
    case "Acht" => 8.0
    case "Neun" => 9.0
    case "Zehn" => 10.0
    case "Bube" => 11.0
    case "Dame" => 12.0
    case "König" => 13.0
    //Numbers
    case "1" => 1.0
    case "2" => 2.0
    case "3" => 3.0
    case "4" => 4.0
    case "5" => 5.0
    case "6" => 6.0
    case "7" => 7.0
    case "8" => 8.0
    case "9" => 9.0
    case "10" => 10.0
    case _ => 0.0
  }


  private def checkOnDuplicates(cardPairArray: Array[String]): Boolean = {
    if (cardPairArray.size == 5) {
      val list = List(cardPairArray(0), cardPairArray(1), cardPairArray(2), cardPairArray(3), cardPairArray(4))
      val listTest = cardPairArray.toList

      val distinct = listTest.distinct //Remove equal Inputs(equal Cards)

      var result = list.size - distinct.size
      if (result != 0) {
        result += 1
        println("!Invalid Poker Hand Input -[" + result + "] Cards are equal")
        return false
      }
      return true
    } else {
      val checkUp = cardPairArray(0).split(",")
      if (getSuitDoubleValue(checkUp(0)) == 0.0) {
        println("!Invalid Input \" " + cardPairArray(0) + " \" -  Please enter 5 Card Pairs")
      } else {
        println("!Invalid Poker Hand Input - Entered: [" + cardPairArray.size + " Card Pairs] - Valid Poker Hand: [5 Card Pairs]")
      }

      return false
    }
  }

}

//End of Class




