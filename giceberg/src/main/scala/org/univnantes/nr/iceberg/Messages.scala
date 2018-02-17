package org.univnantes.nr.iceberg

import org.univnantes.nr.sample.ASample
import scala.collection.mutable.HashMap

case class Identify(s: Int, m: Int, freq: HashMap[ASample, Int])
case class FreqRequest(samples: List[ASample], sn: Int)
case class FreqReply(s: Int, m: Int, freq: HashMap[ASample, Int], sn: Int)