package org.univnantes.nr.iceberg.omni

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.Random
import org.univnantes.nr.sample.ASample
import org.univnantes.nr.sample.SampleConsumer
import akka.actor.Actor
import akka.actor.ActorRef
import org.univnantes.nr.tools.logging.Logger
import akka.actor.actorRef2Scala
import org.univnantes.nr.iceberg.FreqRequest
import org.univnantes.nr.iceberg.FreqReply
import org.univnantes.nr.iceberg.Identify
import akka.actor.Props
import akka.actor.ActorSystem
import org.univnantes.nr.iceberg.coordinator.Coordinator
import org.univnantes.nr.distributions.TDistribution
import org.univnantes.nr.prng.TPRNG
import org.apache.commons.math3.random.RandomDataImpl

object Omniscient {

  def getL(sSize: Int) = (Math.floor(Math.log(sSize)) + 1).toInt

  def createProbes(actorSystem: ActorSystem, c: Array[Int], sSize: Int, thetha: Double, coord: ActorRef, prngFunction: () => TPRNG) = {

    val omniscientProbes = new HashSet[ActorRef]
    var count = 0
    0 to sSize - 1 foreach { _ =>
      {
        val actor = actorSystem.actorOf(Omniscient.props(c, count, sSize, thetha, coord, prngFunction()), "omniscient" + count)
        omniscientProbes.add(actor)
        coord ! Coordinator.AddProbe(actor)
        count += 1
      }
    }
    omniscientProbes
  }

  def killProbes(actorSystem: ActorSystem, probes: HashSet[ActorRef]) {
    probes.foreach(probe => actorSystem.stop(probe))
  }

  def props(c: Array[Int], s: Int, sSize: Int, thetha: Double, coord: ActorRef, probability: TPRNG): Props =
    Props(classOf[Omniscient], c, s, sSize, thetha, coord, probability)

}

class Omniscient(c: Array[Int], s: Int, sSize: Int, thetha: Double, coord: ActorRef, probability: TPRNG) extends SampleConsumer with Actor with Logger {
  val minp = probability.getDistributionVector.min

  val generator = new RandomDataImpl()
  val l = Omniscient.getL(sSize)
  var m = 0
  val f = new HashMap[ASample, Int] // F[j]
  val gamma = Array.fill[HashSet[ASample]](l) { new HashSet[ASample] } // gammma_k
  var global_m = 0
  logger.info("Created Omniscient Probe %d, minp: %f, l: %d, c: %s".format(s, minp, l, c.mkString(",")))
  def receive = {

    case sample: ASample => {
      logger.debug("[%d] Received Sample: %s".format(s, sample))
      val prob = probability.getProbability(sample)
      m += 1 // ms <- ms +1
      f.put(sample, f.getOrElse(sample, 0) + 1) // F[j] <- F[j] + 1

      if (prob > thetha) { // if pj > thetha
        /*if (m % 50 == 0)
          println()*/
        if (m % 1000 == 0)
          println(m)
        //print("+")
        logger.debug("[%d] prob  = %f > %f = thetha".format(s, prob, thetha))
        val rand = generator.nextUniform(0, 1)
        val a = minp / prob
       // print("*")
        if (rand <= a) { // with probability aj do
          print(sample)
          logger.debug("[%d] a = %f >=  rand = %f".format(s, a, rand))
          var k = 0
          val guard = Math.pow(2, l - 1) * thetha
          if (prob >= guard) { // if pj > thetha * 2 ^(l-1)
           //print("1")
            logger.debug("[%d] prob = %f >= guard = %f => k = l = %d".format(s, a, guard, l))
            k = l // k <- l
          } else {
           // print("2")
            logger.debug("[%d] prob = %f < guard = %f".format(s, a, guard))
            k = 1
            var left = Math.pow(2, k - 1) * thetha
            var right = Math.pow(2, k) * thetha

            while (!(left <= prob && prob < right) && k < l) {
              k += 1
              left = right
              right = Math.pow(2, k) * thetha
            }
            logger.debug("[%d] k = %d. ".format(s, k))

          }

          k -= 1 // translate to array index

          //val local_gamma = gamma(k)
          if (!gamma(k).contains(sample)) { // if j not in Gammak
            logger.debug("[%d] local_gamma did not contain the sample: %s".format(s, sample))
            gamma(k).add(sample) // Gammak <- Gammak + j
            //print("^ (gamma(%d) size = %d, limit = %d)".format(k, gamma(k).size, c(k)))
            if (gamma(k).size == c(k)) {
              //print("%")
              logger.debug("[%d] local_gamma size = %d == c(%d)  = %d".format(s, gamma(k).size, k, c(k)))
              val freq = HashMap[ASample, Int]()
              gamma(k).foreach(sample => { // foreach j in Gammak do
                freq.put(sample, f.getOrElse(sample, 0)) // Freq <- Freq + (j, F[j])
              })
              val msg = Identify(s, m, freq)
              logger.debug("################################# ")
              logger.debug("################################# [%d] Sending message to coord: %s".format(s, msg))
              logger.debug("################################# ")
              coord ! msg // Send message (identify, s, ms, Freq) to coord
              gamma(k).clear // Gammak <- empty, Freq <- empty
               
            } else {
              logger.debug("[%d] local_gamma size = %d  < c(%d) = %d".format(s, gamma(k).size, k, c(k)))
            }
          } else {
            logger.debug("[%d] local_gamma did contain the sample: %s".format(s, sample))
          }

        } else {
          logger.debug("[%d] a = %f <  %f = rand".format(s, a, rand))
        }
      } else {
        logger.debug("[%d] prob  = %f <= theta = %f".format(s, prob, thetha))
      }
    }

    case FreqRequest(samples, sn) => {
      logger.debug("Received message FreqRequest %s, %d, %s".format(samples.mkString(" | "), sn, f.mkString(" | ")))
      val freq = new HashMap[ASample, Int]
      samples.foreach(sample => { // foreach j in Freq do  
        freq.put(sample, f.getOrElse(sample, 0)) // Freq <- (j, F[j])
      })
      logger.debug("Replying to message FreqRequest %d, %s".format(sn, freq.mkString(" | ")))
      coord ! FreqReply(s, m, freq, sn) // Send message (freq, s, ms, Freq) to coord
    }

    case _ => { logger.warn("Unknown message") }
  }

  def newSample(sample: ASample) {
    self ! sample
  }

  def receiveCoordinationMessage(msg: FreqRequest) {
    self ! msg
  }

}