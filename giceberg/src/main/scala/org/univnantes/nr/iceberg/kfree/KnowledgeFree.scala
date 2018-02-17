package org.univnantes.nr.iceberg.kfree

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.commons.math3.random.RandomDataImpl
import org.univnantes.nr.countmin.CountMin
import org.univnantes.nr.countminImp.CountMinImpr
import org.univnantes.nr.iceberg.FreqReply
import org.univnantes.nr.iceberg.FreqRequest
import org.univnantes.nr.iceberg.Identify
import org.univnantes.nr.sample.ASample
import org.univnantes.nr.sample.SampleConsumer
import org.univnantes.nr.tools.logging.Logger
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import akka.actor.Props
import akka.actor.ActorSystem
import org.univnantes.nr.prng.TPRNG
import org.univnantes.nr.iceberg.coordinator.Coordinator
import org.univnantes.nr.simulation.messages.SimulationEnd
import sun.security.util.Length
import org.univnantes.nr.sample.Sample

object KnowledgeFree {

  def getL(sSize: Int) = (Math.floor(Math.log(sSize) / Math.log(2)) + 1).toInt

  def createProbes(actorSystem: ActorSystem, c: Array[Int], sSize: Int, thetha: Double, coord: ActorRef, domainSize: Int,
    epsilon: Double, delta: Double, k: Int) = {

    val kfProbes = new HashSet[ActorRef]
    var count = 0
    0 to sSize - 1 foreach { _ =>
      {
        val actor = actorSystem.actorOf(KnowledgeFree.props(c, count, sSize, thetha, coord, domainSize, epsilon, delta, k), "kf" + count)
        kfProbes.add(actor)
        coord ! Coordinator.AddProbe(actor)
        count += 1
      }
    }
    kfProbes
  }

  def killProbes(actorSystem: ActorSystem, probes: HashSet[ActorRef]) {
    probes.foreach(probe => actorSystem.stop(probe))
  }

  def props(c: Array[Int], s: Int, sSize: Int, thetha: Double, coord: ActorRef, domainSize: Int,
    epsilon: Double, delta: Double, k: Int): Props =
    Props(classOf[KnowledgeFree], c, s, sSize, thetha, coord, domainSize, epsilon, delta, k)

  def gammaGuards(thetha: Double, l: Int): Array[Double] = {
    val gammaGuards = new Array[Double](l)
    gammaGuards(0) = thetha

    1 to gammaGuards.length - 1 foreach (i => {
      gammaGuards(i) = thetha + (1 - thetha) / Math.pow(2, l - i)
    })
    gammaGuards
  }

  def getc(r: Double, gammaGuards: Array[Double]): Array[Int] = {
    val c = new Array[Int](gammaGuards.length)
    0 to c.length - 1 foreach (i => {
      val size = Math.floor(r / gammaGuards(i)).toInt
      if (size < 1) {
        c(i) = 1
      } else {
        c(i) = size
      }
    })
    return c
  }

}

class KnowledgeFree(c: Array[Int], s: Int, sSize: Int, thetha: Double, coord: ActorRef, domainSize: Int,
  epsilon: Double, delta: Double, k: Int) extends SampleConsumer with Actor with Logger {

  val countMin = new CountMinImpr(domainSize, epsilon, delta, k)

  val generator = new RandomDataImpl()
  val l = KnowledgeFree.getL(sSize)
  var m = 0
  val f = new HashMap[ASample, Int] // F[j]
  val gamma = Array.fill[HashSet[ASample]](l) { new HashSet[ASample] } // gammma_k
  val gammaGuards = KnowledgeFree.gammaGuards(thetha, l).toArray
  val gammaTimeout = Array.fill[Int](l)(0)
  val timeoutSize = 1000

  var global_m = 0
  logger.info("Created KF Probe %d,  l: %d, c: %s".format(s, l, c.mkString(",")))

  var messageCount = 0

  val startTimeSample = new Array[Long](1000)
  val endTimeSample = new Array[Long](1000)
  var countSample = 0
  val startTimeFreqReq = new Array[Long](1000)
  val endTimeFreqReq = new Array[Long](1000)
  var countFreqRequest = 0

  def receive = {

    case SimulationEnd(id) => {
      coord ! Coordinator.GISimulationEnd(s, messageCount)
    }

    case sample: ASample => {

      startTimeSample(countSample % endTimeSample.size) = System.nanoTime()

      logger.debug("[%d] Received Sample: %s".format(s, sample))

      m += 1 // ms <- ms +1
      countMin.newSample(sample)
      val cm = countMin.getMin(sample)

      val prob = cm.toDouble / m.toDouble
      logger.debug("[%d] cm / m = prob : %f / %f = %f".format(s, cm.toDouble, m.toDouble, prob))
      f.put(sample, f.getOrElse(sample, 0) + 1) // F[j] <- F[j] + 1

      val k = getK(prob)

      0 to gamma.length - 1 foreach { i =>
        {
          if (i != k) {
            gamma(i).remove(sample)
          }
        }
      }

      if (prob > thetha) { // if pj > thetha

        logger.debug("[%d] prob  = %f > %f = thetha".format(s, prob, thetha))
        val rand = generator.nextUniform(0, 1)
        val minp = 1 // TODO

        var a_value = 0.0
        val globalMin = countMin.getNonZeroGlobalMin.toDouble
        if (globalMin == 0) {
          a_value = 1.0 / countMin.bjkst.getDistinctElements.toDouble
          logger.error("GlobalMin zero, using bjkst :  a = 1.0/bjkst:  %f = 1.0 / %d".format(a_value, countMin.bjkst.getDistinctElements))
        } else {
          a_value = countMin.getGlobalMin.toDouble / cm.toDouble
        }

        val a = a_value

        if (rand <= a) { // with probability aj do

          logger.debug("[%d] a = %f >=  rand = %f".format(s, a, rand))

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

              //println("################################# [%d] Sending (k=%d) message to coord: %s".format(s, k, msg))

              coord ! msg // Send message (identify, s, ms, Freq) to coord
              messageCount += 1
              gamma(k).clear // Gammak <- empty, Freq <- empty
              gammaTimeout(k) = m

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

      0 to gamma.length - 1 foreach { i =>
        {
          if (!gamma(i).isEmpty && (m - gammaTimeout(i) > timeoutSize)) {
            gammaTimeout(i) = m
            val freq = HashMap[ASample, Int]()
            gamma(i).foreach(sample => { // foreach j in Gammak do
              freq.put(sample, f.getOrElse(sample, 0)) // Freq <- Freq + (j, F[j])
            })
            val msg = Identify(s, m, freq)

            // println("################################# [%d] Sending (i=%d) message to coord: %s".format(s, i, msg))

            coord ! msg // Send message (identify, s, ms, Freq) to coord
            messageCount += 1
            gamma(i).clear
          }
        }
      }

      endTimeSample(countSample % endTimeSample.size) = System.nanoTime()
      if (countSample % startTimeSample.size == 0) {
        var sum = 0L
        0 to startTimeSample.size - 1 foreach (i => {
          sum += endTimeSample(i) - startTimeSample(i)
          endTimeSample(i) = 0L
          startTimeSample(i) = 0L
        })
        sum = sum / (1000 * startTimeFreqReq.size.toLong)
        println("KF Manage Sample (%d) Elapsed Time = %d micros".format(countSample, sum))
      }
      countSample += 1
    }

    case FreqRequest(samples, sn) => {
      startTimeFreqReq(countFreqRequest % startTimeFreqReq.size) = System.nanoTime()

      logger.debug("Received message FreqRequest %s, %d, %s".format(samples.mkString(" | "), sn, f.mkString(" | ")))
      val freq = new HashMap[ASample, Int]
      samples.foreach(sample => { // foreach j in Freq do  
        freq.put(sample, f.getOrElse(sample, 0)) // Freq <- (j, F[j])
      })
      logger.debug("Replying to message FreqRequest %d, %s".format(sn, freq.mkString(" | ")))
      coord ! FreqReply(s, m, freq, sn) // Send message (freq, s, ms, Freq) to coord

      endTimeFreqReq(countFreqRequest % endTimeFreqReq.size) = System.nanoTime()

      if (countFreqRequest % startTimeFreqReq.size == 0) {
        var sum = 0L
        0 to startTimeFreqReq.size - 1 foreach (i => {
          sum += endTimeFreqReq(i) - startTimeFreqReq(i)
          endTimeFreqReq(i) = 0L
          startTimeFreqReq(i) = 0L
        })
        sum = sum / (1000 * startTimeFreqReq.size.toLong)

        println("KF Freq Request (%d) Elapsed Time = %d micros".format(countFreqRequest, sum))
      }

      countFreqRequest += 1
    }

    case _ => { logger.warn("Unknown message") }
  }

  def newSample(sample: ASample) {
    self ! sample
  }

  def receiveCoordinationMessage(msg: FreqRequest) {
    self ! msg
  }

  private def getK(prob: Double): Int = {

    var k = 0

    if (prob < thetha) {
      k = -1
    } else {
      //println("prob: %f".format(prob))
      while (gammaGuards(k) <= prob && k < gammaGuards.length - 1) {
        k += 1
      }
      //println("prob: %f, k: %d".format(prob, k))
    }

    k
  }

  private def getK1(prob: Double): Int = {
    var k = 0
    val guard = Math.pow(2, l - 1) * thetha
    if (prob >= guard) { // if pj > thetha * 2 ^(l-1)

      //logger.debug("[%d] prob = %f >= guard = %f => k = l = %d".format(s, a, guard, l))
      k = l // k <- l
    } else {

      //logger.debug("[%d] prob = %f < guard = %f".format(s, a, guard))
      k = 1
      var left = Math.pow(2, k - 1) * thetha
      var right = 2 * left

      while (!(left <= prob && prob < right) && k < l) {
        k += 1
        left = right
        right = 2 * left
      }
      logger.debug("[%d] k = %d. ".format(s, k))
    }
    k - 1
  }

}