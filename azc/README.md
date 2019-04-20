# AZC - Toke Digital's Azure Blobstore Client

This simple java client can interact with Microsoft Azure blobstores. It works with Ansible as a module as well. 

## Getting Started

Download the jar including dependencies:

<code>
curl -o azc.jar \<maven central\>/azc/1.0.3/azc-1.0.3-jar-with-dependencies.jar
</code>

Create a configuration file like this, called something like azc-config.properties

<code>
 # start file<br/>
AZURE_STORAGE_ACCOUNT=xxxx<br/>
AZURE_STORAGE_ACCESS_KEY=xxxxx<br/>
CONTAINER_NAME=xxxx<br/>
</code>
<br/>
<code>
 # if needed<br/>
USE_PROXY=true<br/>
HTTP_PROXY=proxy.example.com<br/>
HTTPS_PROXY=proxy.example.com<br/>
HTTP_PROXY_PORT=80<br/>
HTTPS_PROXY_PORT=443<br/>

 # end file<br/>
</code>


It is possible to pass in the three values below as environment variables instead of putting them in the config file:

<code>

export AZURE\_STORAGE\_ACCOUNT=xxxx<br/>
export AZURE\_STORAGE\_ACCESS_KEY=xxxx<br/>
export CONTAINER\_NAME=xxxx<br/>

</code>

### Prerequisites

Requires a Java 8 runtime.

## Examples

#### list all the files in the blobstore

java -jar azc.jar --config azc-config.properties --verb list

#### Get an existing file from out of the blobstore

java -jar azc.jar --config azc-config.properties --verb get --file myfile.zip --dest /tmp

#### Get a file in silent mode (no console output)

java -jar azc.jar --config azc-config.properties --verb get --silent --file myfile.zip --dest /tmp


#### Send a file into the blobstore

java -jar azc.jar --config azc-config.properties --verb send --file myfile.zip


#### Get All the files and put them into /tmp

java -jar azc.jar --config azc-config.properties --verb getAll --dest /tmp








