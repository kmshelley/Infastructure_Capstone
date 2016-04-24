# SafeRoad Installation

SafeRoad utilizes the cluster computing engine [Spark](http://spark.apache.org/) and the distributed document store [Elasticsearch](https://www.elastic.co/).

The following instructions are for a cluster of Linux CentOS 7 VMs.

## Update the VM's
Update the installed packages; install git, pip, and a few dependencies.
```
yum -y install epel-release
yum -y update
yum -y install git
yum -y install python-pip
yum -y install gcc
yum -y install bzip2
```

Update the `/etc/hosts` file with all the nodes in the cluster.
```
vi /etc/hosts
```
**Important**: to reduce outbound network traffic always use internal IP addresses when possible.

Setup passwordless ssh between all the nodes. Leave the password field blank for passwordless ssh. Replace `<node names>` with the hostnames of the nodes in your cluster.

```
ssh-keygen
for i in <node names>; do ssh-copy-id $i; done
```
Verify that you can ssh to each node without a password. Replace `<node names>` with the hostnames of the nodes in your cluster.

```
for i in <node names>; do ssh $i; done
```

## Install Anaconda (Optional)
We recommend installing the scientific Python distribution, Anaconda.
```
wget http://repo.continuum.io/archive/Anaconda2-2.5.0-Linux-x86_64.sh
bash Anaconda2-2.5.0-Linux-x86_64.sh
source ~/.bashrc
```

## Install Java
Both Spark and Elasticsearch require an installation of Java.
```
curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
yum install -y java-1.8.0-openjdk-headless sbt
echo export JAVA_HOME=\"$(readlink -f $(which java) | grep -oP '.*(?=/bin)')\" >> /root/.bash_profile
source /root/.bash_profile
```

Verify the installation was successful.
```
$JAVA_HOME/bin/java -version
```
## Install Spark 1.6
```
wget http://mirror.symnds.com/software/Apache/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
tar -zxvf spark-1.6.1-bin-hadoop2.6.tgz
mv spark-1.6.1-bin-hadoop2.6 /usr/local/spark16
```

Define `$SPARK_HOME`.
```
echo export SPARK_HOME=\"/usr/local/spark16\" >> /root/.bash_profile
source /root/.bash_profile
```

On the Spark **master node only** we will add the host names of the slave nodes to `$SPARK_HOME/conf/slaves`.
```
cd $SPARK_HOME
vi conf/slaves
```

Launch the Spark cluster. To verify the installation was successful, navigate to `http://<masterIP>:8080`.
```
sbin/start-all.sh
```

## Setup and Install Elasticsearch

Elasticsearch recommends large disk space; we recommend a VM with a minimum of 100GB of disk space.

Find the 100GB disk:
```
fdisk -l |grep Disk |grep GB
```
Assuming your disk is called /dev/xvdc:
```
fdisk /dev/xvdc
sudo mkfs -t ext3 /dev/xvdc1
```

Create directories for Elasticsearch data and log files. Mount the disk.
```
mkdir /media/elasticsearch
mount /dev/xvdc1 /media/elasticsearch
mkdir /media/elasticsearch/logs1
mkdir /media/elasticsearch/data1
```

Elasticsearch won't let you launch as root so we will create a new user.
```
groupadd elasticsearch
useradd -g elasticsearch elasticsearch
chown -R elasticsearch:elasticsearch /media/elasticsearch
cp /etc/sudoers /etc/sudoers.backup
echo "elasticsearch ALL=(ALL:ALL) ALL" >> /etc/sudoers
mkdir /home/elasticsearch
chown -R elasticsearch:elasticsearch /home/elasticsearch
```

Set the password for elasticsearch:
```
passwd elasticsearch
```

We will now switch to the elasticsearch user for the remainder of the setup.
```
su - elasticsearch
```

Download and install Elasticsearch.
```
cd
curl -OL https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/deb/elasticsearch/2.2.0/elasticsearch-2.2.0.deb
sudo dpkg -i elasticsearch-2.2.0.deb
```

Define Elasticsearch memory parameters.
```
echo "export ES_MIN_MEM=32g" >> ~/.bash_profile
echo "export ES_MAX_MEM=32g" >> ~/.bash_profile
echo "export ES_HEAP_SIZE=32g" >> ~/.bash_profile
chmod 700 ~/.bash_profile
source ~/.bash_profile
```

Make sure the elasticsearch user owns the various Elasticsearch directories.
```

chown -R elasticsearch:elasticsearch /etc/elasticsearch
chown -R elasticsearch:elasticsearch /usr/share/elasticsearch
chown -R elasticsearch:elasticsearch /var
```

Next we will install Shield which will allow us to set a username and password for our cluster. Replace `<username>` with your username. You will be prompted for a password.

```
/usr/share/elasticsearch/bin/plugin install license
/usr/share/elasticsearch/bin/plugin install shield
/usr/share/elasticsearch/bin/plugin install marvel-agent
sudo shield/esusers useradd <username> -r admin
```

Modify elasticsearch.yml. Set the master and network host to the master's IP address.
```
vi /etc/elasticsearch/config/elasticsearch.yml
```

Launch Elasticsearch and switch back to the root user.
```
sudo /etc/init.d/elasticsearch start
su -
```

Verify the installation; check the cluster health. Replace `<username>` and `<passwd>` with the cluster username and password and `<masterIP>` with the IP address of the master node.
```
curl -u <username>:<passwd> -XGET   'http://<masterIP>:9200/_cluster/health?pretty=true'
```

## Install Hadoop-Elasticsearch Plugin
Next we need to install a plugin in order for Spark to interact with the Elasticsearch cluster.

```
wget http://download.elastic.co/hadoop/elasticsearch-hadoop-2.2.0.zip
tar -zxvf elasticsearch-hadoop-2.2.0.zip
mv elasticsearch-hadoop-2.2.0.jar $SPARK_HOME/jars/
```


## Jupyter Notebook
To Launch Jupyter Notebook Server with Spark kernel:
```
IPYTHON_OPTS="notebook " $SPARK_HOME/bin/pyspark --master spark://<spark-master>:7077 --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.2.0.jar
```
## Finally...
Clone this repo!
```
cd
git clone https://github.com/kmshelley/SafeRoad
cd SafeRoad
pip install -r requirements.txt
```

Modify the configuration file with the definitions for the Elasticsearch cluster, Spark, and any API keys necessary to access Open Data API's:
```
vi config/capstone_config.ini
```

API keys can be obtained here:

* Socrata Open Data API: https://dev.socrata.com/
* Weather Underground API: https://www.wunderground.com/weather/api/


We have made our original NYC collision feature grid and predictions results grid as well and other miscellaneous data files available as JSON files in the directory `flatDataFiles`. To reload the data run:

```
python start_up.py
```

Finally, run the main program:

```
python main.py
```

We hope you enjoy our tool. Drive safe!
