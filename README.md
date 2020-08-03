# NetWolf
It's a P2P torrent-like network system.<br />
* Discovering cluster list nodes via sending own cluster list to nodes in cluster.
* Getting files via sending the name of the file to cluster nodes, then choose the less delay in connections.
* Discovering and Get request are in UDP and getting files are in TCP.

<br />**Commands:**<br />
```list``` for seeing cluster list, ```get <file_name>``` for searching for a file in cluster nodes and getting it if it was available.
