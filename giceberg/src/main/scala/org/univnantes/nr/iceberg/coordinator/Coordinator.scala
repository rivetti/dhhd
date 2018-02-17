package org.univnantes.nr.iceberg.coordinator

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.SynchronizedQueue
import org.univnantes.nr.iceberg.FreqReply
import org.univnantes.nr.iceberg.Identify
import org.univnantes.nr.sample.ASample
import org.univnantes.nr.tools.logging.Logger
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Stash
import akka.actor.actorRef2Scala
import org.univnantes.nr.iceberg.FreqRequest
import akka.actor.Props
import akka.dispatch.Mailbox
import org.univnantes.nr.simulation.messages.SimulationEnd
import scala.collection.mutable.SynchronizedSet

object Coordinator {

  var fs: List[(Int, HashMap[ASample, Int])] = Nil
  var identityMsgSize = 0
  var identityMsgCount = 0
  var freqReqMsgSize = 0
  var freqReqMsgCount = 0
  var freqRepMsgSize = 0
  var freqRepMsgCount = 0
  var terminated = false

  case class AddProbe(probe: ActorRef)
  case class GISimulationEnd(id: Int, msg: Int)

  def props(thetha: Double): Props =
    Props(classOf[Coordinator], thetha)

  def storeF(m: Int, f: HashMap[ASample, Int]) {
    fs = (m, f) :: fs
  }

  def retrieveF() = fs

  def printF() = fs.mkString("\n")

  def retrieveResults(): (Int, Int, Int, Int, Int, Int) = {
    (identityMsgSize, identityMsgCount, freqReqMsgSize, freqReqMsgCount, freqRepMsgSize, freqRepMsgCount)
  }

  def isTerminated() = terminated

  def waitTermination() {
    while (!terminated)
      Thread.sleep(5000)
  }
}

class Coordinator(thetha: Double) extends Actor with Stash with Logger {

  Coordinator.terminated = false
  Coordinator.identityMsgSize = 0
  Coordinator.identityMsgCount = 0
  Coordinator.freqReqMsgSize = 0
  Coordinator.freqReqMsgCount = 0
  Coordinator.freqRepMsgSize = 0
  Coordinator.freqRepMsgCount = 0
  Coordinator.fs = Nil

  var probesTerminated = false
  val error = 10

  val probes = new HashSet[ActorRef]
  val runnningProbes = new HashSet[ActorRef] with SynchronizedSet[ActorRef]
  var m = 0
  var currSn = 0;
  var waitingFor = new HashSet[ActorRef]()
  var f = new HashMap[ASample, Int]()

  // START Perf Debug  
  val startTimeIdentity = new Array[Long](1000)
  val endTimeIdentity = new Array[Long](1000)
  var countIdentity = 0
  val startTimeFreqReq = new Array[Long](1000)
  val endTimeFreqReq = new Array[Long](1000)
  var countFreqRequest = 0

  val timeFreqReqIssue = new Array[Long](100)
  val timeFreqReqReply = new Array[Long](100)
  var countFreqRequestReply = 0

  val startTimeBecome = new Array[Long](1000)
  val endTimeBecome = new Array[Long](1000)
  var countBecome = 0

  // END Perf Debug

  var msgFromProbes = 0
  val distinctSampleCount = new HashMap[Int, Int]
  var sampleCount = 0
  var prevSampeCount = 0

  def receive() = {

    case Coordinator.AddProbe(probe) => {
      probes.add(probe)
      runnningProbes.add(probe)
    }

    case Coordinator.GISimulationEnd(id, msg) => {

      if (runnningProbes.contains(sender)) {
        msgFromProbes += msg
        runnningProbes.remove(sender)
        if (runnningProbes.isEmpty) {

          println("Coordinator Terminated: CoordCount: %d, ProbesCount: %d".format(Coordinator.identityMsgCount, msgFromProbes))
          Coordinator.terminated = true

        }
      }
    }

    case Identify(s, ms, freq) => {

      // START Perf Debug
      startTimeIdentity(countIdentity % startTimeIdentity.size) = System.nanoTime()
      // END Perf Debug      

      sampleCount = sampleCount + (ms - distinctSampleCount.getOrElse(s, 0))
      Coordinator.identityMsgSize += freq.size
      Coordinator.identityMsgCount += 1

      logger.info("Received message Identify %d, %d, %s".format(s, ms, freq.mkString(" | ")))
      m = ms // m <- ms
      f = freq // F <- freq

      distinctSampleCount.put(s, ms)
      sampleCount = distinctSampleCount.foldLeft(0)((x, y) => { x + y._2 })

      if (prevSampeCount + 50000 <= sampleCount) {
        prevSampeCount = sampleCount
        println("Coord sampleCount = %d, msgCount = %d".format(sampleCount, Coordinator.identityMsgCount))
      }

      var samples: List[ASample] = Nil
      f.foreach(pair => { // foreach (k, fk) in Freq
        samples = pair._1 :: samples // (k, fk) <- (k, null)
      })

      waitingFor = probes.filter(_ != sender)
      logger.debug("Sending FreqRequest to probes")
      waitingFor.foreach(probe => { // foreach s' != s in {1, ..., S} do
        Coordinator.freqReqMsgSize += samples.size
        Coordinator.freqReqMsgCount += 1
        probe ! FreqRequest(samples, currSn) // send message ("freq?", Freq)

        timeFreqReqIssue(countFreqRequestReply % timeFreqReqIssue.size) = System.nanoTime()
      })
      logger.debug("Sent FreqRequest to probes")

      unstashAll()

      // START Perf Debug
      endTimeIdentity(countIdentity % endTimeIdentity.size) = System.nanoTime()
      if (countIdentity % startTimeIdentity.size == 0) {
        var sum = 0L
        0 to startTimeIdentity.size - 1 foreach (i => {
          sum += endTimeIdentity(i) - startTimeIdentity(i)
          endTimeIdentity(i) = 0L
          startTimeIdentity(i) = 0L
        })
        sum = sum / (1000 * startTimeIdentity.size.toLong)
        println("Coord Identity (%d) Elapsed Time = %d micros".format(countIdentity, sum))
      }
      countIdentity += 1
      // END Perf Debug

      // START Perf Debug
     // startTimeBecome(countBecome % startTimeBecome.size) = System.nanoTime()
      // END Perf Debug 

      context.become(waitingReplyBehaviour, false)
    }
    case msg: FreqReply => { logger.warn("Received FreqReply, but no such message should be trasnmitted now: %s".format(msg)) }
    case msg => { logger.warn("Unknown message: %s".format(msg)) }

  }

  val waitingReplyBehaviour: PartialFunction[Any, Unit] = {

    case Coordinator.GISimulationEnd(id, msg) => stash()

    case msg: Identify => stash()

    case FreqReply(s, ms, freq, sn) if sn == currSn => { // foreach message ("freq", s', ms', Freq)

      // START Perf Debug
      startTimeFreqReq(countFreqRequest % startTimeFreqReq.size) = System.nanoTime()
      // END Perf Debug

      Coordinator.freqRepMsgSize += freq.size
      Coordinator.freqRepMsgCount += 1

      logger.debug("Received message FreqReply %d, %d, %s, %d".format(s, ms, freq.mkString(" | "), sn))
      m += ms // m <- m + ms

      freq.foreach(pair => { // foreach (k, fk) in F do
        f.put(pair._1, pair._2 + f.get(pair._1).getOrElse(0)) // Update F: fk <- fk + fk' with (k, fk') in Freq
      })

      waitingFor.remove(sender)
      if (waitingFor.size == 0) {
        // START Perf Debug
        timeFreqReqReply(countFreqRequestReply % timeFreqReqReply.size) = System.nanoTime()

        if (countFreqRequestReply % timeFreqReqIssue.size == 0) {
          var sum = 0L
          0 to timeFreqReqIssue.size - 1 foreach (i => {
            sum += timeFreqReqReply(i) - timeFreqReqIssue(i)
            timeFreqReqReply(i) = 0L
            timeFreqReqIssue(i) = 0L
          })
          sum = sum / (1000 * timeFreqReqIssue.size.toLong)
          println("Coord Freq ReqReply (%d) Elapsed Time = %d micros".format(countFreqRequestReply, sum))
        }
        countFreqRequestReply += 1
        // END Perf Debug

        val threshold = Math.round(m * thetha).toInt
        logger.debug("Compute new F, threshold = %d, candidate: %s".format(threshold, f.mkString(" | ")))
        val res = f.filterNot(pair => { pair._2 < threshold }) // foreach (k, fk) in F do, if fk < m * theta then F <- F \ (k, fk)
        Coordinator.storeF(m, res)
        currSn += 1
        unstashAll()
        context.unbecome
      }

      // START Perf Debug
      endTimeFreqReq(countFreqRequest % endTimeFreqReq.size) = System.nanoTime()
      if (countFreqRequest % startTimeFreqReq.size == 0) {
        var sum = 0L
        0 to startTimeFreqReq.size - 1 foreach (i => {
          sum += endTimeFreqReq(i) - startTimeFreqReq(i)
          endTimeFreqReq(i) = 0L
          startTimeFreqReq(i) = 0L
        })
        sum = sum / (1000 * startTimeFreqReq.size.toLong)
        println("Coord Freq Reply (%d) Elapsed Time = %d micros".format(countFreqRequest, sum))
      }
      countFreqRequest += 1
      // END Perf Debug
    }

    case msg: FreqReply => { logger.warn("Received FreqReply, but the sn did not match: %s".format(msg)) }
    case msg => { logger.warn("Unknown message: %s".format(msg)) }
  }

}