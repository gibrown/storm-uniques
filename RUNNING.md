Locally
=======

~~~bash
$ mvn -f m2-pom.xml compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.starter.trident.TridentUniques
~~~

Remotely
========

Initial Setup
-------------

~~~bash
$ mkdir ~/.storm
$ echo 'nimbus.host: "192.168.101.11"' > ~/.storm/storm.yaml # Adjust the IP address as needed
~~~

Submit
------
~~~bash
$ mvn -f m2-pom.xml package
$ ./tools/submit CLASS NAME
# ./tools/submit storm.starter.trident.TridentUniques Uniques

$ ./tools/kill NAME
# ./tools/kill Uniques
~~~
