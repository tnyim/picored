<img src="http://s.lowendshare.com/3/1428615072.301.picored%20logo.png">

**PicoRed - Distributed Server Redundancy Manager**

PicoRed manages servers and applications in a cluster, to ensure the applications' availability in case of failure of one of the members of the cluster.

It is being developed by the tny. internet media group ( http://i.tny.im ) with the intent of managing a few servers that serve identical copies of a website. In our case, PicoRed's job will be to add and remove servers from the DNS records (set up in a Round-robin fashion) of that website as the servers' availability changes, with the objective of achieving a global uptime that is higher than the average of the uptime of the servers.

In the long term, it should become a general way to manage small and large server groups whose elements have varying uptime and stability, while maintaining a high application availability.

Currently, PicoRed is designed to run on every machine where the applications to be monitored and managed run. Each PicoRed instance should monitor the application on the machine where it is running, and members of the cluster monitor each other. When a application on a server goes down, or a whole server goes down (i.e., the other elements of the cluster can not contact it), one or more of the PicoRed instances run a event handler, or handler for short.

Handlers are external to PicoRed and are nothing more than scripts or programs that PicoRed will run with different parameters, according to the type of event. As an example, our handlers are simple Python scripts that call an API that sets the DNS records, so that they match with the servers that are up at any given moment.

At this point, handlers will run on any server for an event about any member, and a handler can be called more than once for the same event. We're yet to decide if it should be PicoRed's task to call handlers once and only once, or if it should be the handler's resposiblity to filter the calls. We're erring on the first option, since adding a more strict guarantee would increase complexity, while having handlers run more than once for the same event is not a problem for many of the possible uses of PicoRed.

PicoRed is coded in Go; it uses hashicorp's memberlist ( https://github.com/hashicorp/memberlist ) for much of the magic. PicoRed is intended to be the successor to mersit, a Tiny Server Redundancy Manager written in Python ( http://gbl08ma.com/distributed-systems-and-mersit-a-tiny-server-redundancy-manager/ ) which wasn't ever released to public. While reliable, mersit is not very maintainable, and more importantly, it consumes way too much resources for what it does: PicoRed consumes roughly 10 MB of RAM per instance (server) to do the same thing mersit did, while mersit consumed over 70 MB and with much higher CPU and bandwidth usage. This is important to us as the servers we want to manage are resource-constrained.

Settings and handler examples will be shown here once the project matures.
