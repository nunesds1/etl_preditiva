#!/bin/bash

set -e

echo ">>> Exporting Hadoop environment..."
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

ROLE="${HADOOP_ROLE:-namenode}"   # padrão: namenode

echo "Java version:"
java -version || true

echo ">>> ROLE = $ROLE"

echo ">>> Ensuring required directories exist..."
mkdir -p /opt/hadoop/data/nn
mkdir -p /opt/hadoop/data/dn
mkdir -p /opt/hadoop/logs

echo ">>> Fixing permissions..."
chown -R root:root /opt/hadoop
chmod -R 755 /opt/hadoop/data
chmod -R 755 /opt/hadoop/logs

echo ">>> Starting SSH..."
service ssh start

# (Opcional) chaves SSH – não atrapalha
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
fi

if [ "$ROLE" = "namenode" ]; then
    # Format NameNode apenas na primeira vez
    if [ ! -d "/opt/hadoop/data/nn/current" ]; then
        echo "🆕 Formatting NameNode (first run)..."
        hdfs namenode -format -force
    else
        echo "✔ NameNode already formatted. Skipping format."
    fi

    echo "🚀 Starting HDFS daemons (NN + 2NN)..."
    hdfs --daemon start namenode
    hdfs --daemon start secondarynamenode
    
    echo "🚀 Starting YARN daemons (RM + NM)..."
    yarn --daemon start resourcemanager
    yarn --daemon start nodemanager

elif [ "$ROLE" = "datanode" ]; then
    echo "🚀 Starting DataNode daemon..."
    hdfs --daemon start datanode

    echo "🚀 Starting NodeManager daemon..."
    yarn --daemon start nodemanager
else
    echo "❌ Unknown HADOOP_ROLE: $ROLE"
    exit 1
fi

echo ">>> JVMs running:"
jps

echo "🌍 If this is the NameNode container:"
echo "    - NameNode UI:       http://localhost:9870"
echo "    - YARN UI (RM):      http://localhost:8088"

echo ">>> Tail logs to keep container alive..."
tail -F /opt/hadoop/logs/* 2>/dev/null
