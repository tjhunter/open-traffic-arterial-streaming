open-traffic-arterial-streaming
===============================

An estimation algorithm using Spark Streaming.

Prerequisites
--------------

The code has been tested with scala 2.9.x and spark 0.7.0. I assume you have the following libraries and programs installed:

- simple-build-tool (sbt) version >= 0.12
- spark installed from sources version == 0.7.0 (may work with some later versions)

All the other dependencies should be pulled by sbt.

Compiling
---------

Compiling should be a matter of:

```bash
sbt compile
```