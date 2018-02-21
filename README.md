# dhhd
Distributed Heavy Hitter Detector (DHHD) prototype

This repository contains the implementation of the Distributed Heavy Hitter Detector (DHHD) prototype.

We present the details of this algorithm in the paper Emmanuelle Anceaume, Yann Busnel, Nicolò, Bruno Sericola, "Identifying Global Icebergs in Distributed Streams", IEEE 34th Symposium on Reliable Distributed Systems (SRDS) 2015.


Abstract:
We consider the problem of identifying global iceberg attacks in massive and physically distributed streams. A global iceberg is a distributed denial of service attack, where some elements globally recur many times across the distributed streams, but locally, they do not appear as a deny of service. A natural solution to defend against global iceberg attacks is to rely on multiple routers that locally scan their network traffic, and regularly provide monitoring information to a server in charge of collecting and aggregating all the monitored information. Any relevant solution to this problem must minimise the communication between the routers and the coordinator, and the space required by each node to analyse its stream. We propose a distributed algorithm that tracks global icebergs on the fly with guaranteed error bounds, limited memory and processing requirements. We present a thorough analysis of our algorithm performance. In particular we derive a tight upper bound on the number of bits communicated between the multiple routers and the coordinator in presence of an oblivious adversary. Finally, we present the main results of the experiments we have run on a cluster of single-board computers. Those experiments confirm the efficiency and accuracy of our algorithm to track global icebergs hidden in very large input data streams exhibiting different shapes.


