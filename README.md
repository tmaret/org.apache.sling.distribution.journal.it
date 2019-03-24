org.apache.sling.distribution.journal.it
==========================================

Toxiproxy
---------

For some of the tests a local [toxiproxy installation](https://github.com/Shopify/toxiproxy#1-installing-toxiproxy) is needed.

Unfortunately the docker version of toxiproxy does not work correctly on mac. It would need --net=host which does not work on mac as docker is run in a vm. So currently you need to install toxiproxy using brew manually and start it before the tests.

If toxiproxy is not present the respective tests will be skipped with a message that toxiproxy server is missing.
Because of a bug in pax exam the tests simply succeeed. So be aware that they might not check anything.

