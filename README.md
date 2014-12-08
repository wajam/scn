# SCN

## Description

Distributed sequence and timestamp generator. Generates sequence numbers or timestamp with no collision with Zookeeper to
store its waypoints. It's able to generate 10000 IDs by millisecond and has been tested in production for two years.

![SCN Architecture](http://img11.hostingpics.net/pics/507848ScreenShot20141208at112545AM.png)

## Requirements
- At least three servers for ZooKeeper with 2GB min. The SCN server can run on one of them (512MB min).
- Java 7+.
- SBT 0.13.0+.



## Installation on Ubuntu 14.04 (12.04 should be the same but not tested)
The process will be done in three steps:
  1. Installation of ZooKeeper's cluster.
  2. Export ZooKeeper config via the ZooKeeperClusterTool.
  3. Installation of SCN.
 
#### Installation of ZooKeeper (on three servers at least, you have to repeat theses commands for each server)
1. `sudo apt-get update`
2. `cd ~ && wget http://mirror.csclub.uwaterloo.ca/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz`
4. `tar -xvf zookeeper-3.4.6.tar.gz`
5. `sudo nano ~/zookeeper-3.4.6/conf/zoo.cfg` Use the Annexes, section C.
6. `sudo mkdir -p /var/lib/zookeeper`
7. `sudo nano /var/lib/zookeeper/myid` Type X  where X is the number of your server in zoo.cfg (1, 2 or 3 from zoo.cfg) then save your file.
8. `cd ~/zookeeper-3.4.6`
9. `sudo bin/zkServer.sh start`
10. (Optional) `bin/zkCli.sh -server yourip:2181` Verify if ZooKeeper is running.

#### Export ZooKeeper config (will export your config on every ZooKeeper server)
1. `cd ~ ; wget https://github.com/wajam/nrv/archive/master.zip`
2. `unzip master` (sudo apt-get install -y unzip if you don’t have).
3. `cd nrv-master/ ; sbt stage`
4. `sudo nano local.cluster` Use the Annexes, section D. Careful to modify the IP and PORT to your convenience. If you need more than one SCN server, repeat the two line with the IP and the /votes.
4. `./nrv-zookeeper/target/start` to see the help and do a update with your local.cluster file config. Everything should be fine if you see your config file below the `Add` section.

Example of result:
```
Remove

Add
/services/scn=scn
/services/scn/members/0=0:10.24.130.7:nrv=9595
/services/scn/members/0/votes

Update

Ignore
```

#### Installation of SCN (on one server)
1. `wget https://github.com/wajam/scn/archive/master.zip && unzip master`
2. `cd scn-master && sbt stage`
3. `sudo nano ./etc/default.properties` and modify two lines
  * `scn.nrv.zookeeper.servers=YOURSERVER1,YOURSERVER2,etc.`
  * `scn.storage = zookeeper`
4. `./bin/start -Dscn.config=etc/default.properties com.wajam.scn.ScnServer`

## Usage
The SCN server expose by default an API on port 9500. There is only two calls:
  * `/timestamps/:name/next` Return IDs following the current timestamp.
  * `/sequences/:name/next` Return IDs starting by 0 (or the number given in your the default.properties).

If you want to have more than one ID, you can use the query param `length`:
  * `/timestamps/:name/next?length=1000`
  * `/sequences/:name/next?length=1000`

## FAQ
##### I have a `class org.apache.zookeeper.KeeperException$NoNodeException`, what did I do wrong?
Either you forgot to modify the `scn.nrv.zookeeper.servers` in the default.properties or you didn't start your ZooKeeper servers.

##### I want to have more than one SCN server, how do I do that?
Follow the `Installation of SCN` for every new SCN server and then just add theses line for each SCN server in local.cluster in the `Export ZooKeeper Config`:
  * `/services/scn/members/0=0:YOURNRVCLIENTIP:nrv=YOURNRVPORT (default 9595)`
  * `/services/scn/members/0/votes`

Your local.cluster should be like that:
```
/services/scn=scn

/services/scn/members/0=0:YOURNRVCLIENTIP1:nrv=YOURNRVPORT
/services/scn/members/0/votes

/services/scn/members/0=0:YOURNRVCLIENTIP2:nrv=YOURNRVPORT
/services/scn/members/0/votes

...

/services/scn/members/0=0:YOURNRVCLIENTIPN:nrv=YOURNRVPORT
/services/scn/members/0/votes
```

## Annexes
##### A - Installation of Java 7 OpenJDK (if you prefer another JDK, feel free to install your own but we need Java)
1. `sudo apt-get install openjdk-7-jdk -y` Install OpenJDK 7.
2. `sudo update-alternatives --config java` Select the right version of Java.

##### B - Installation of SBT 0.13.6
  1. `wget https://dl.bintray.com/sbt/debian/sbt-0.13.6.deb`
  2. `sudo dpkg -i sbt-0.13.6.deb`

##### C - zoo.cfg
```
tickTime=2000  
dataDir=/var/lib/zookeeper  
clientPort=2181  
initLimit=5  
syncLimit=2  
server.1=10.0.0.2:2888:3888  
server.2=10.0.0.3:2888:3888  
server.3=10.0.0.4:2888:3888
# Replace IP servers with yours
```
##### D - local.cluster
```
# SCN
/services/scn=scn
/services/scn/members/0=0:YOURNRVCLIENTIP:nrv=YOURNRVPORT (default 9595)
/services/scn/members/0/votes
```
