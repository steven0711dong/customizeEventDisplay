How to create a sink that references a scraper: 

docker build -f Dockerfile . 

After building the image, tag and push to dockerhub. 

The image depends on several env vars:
* scraper: eg. https://scraper.nl51m5ac5j5.dev-pg1.codeengine.dev.appdomain.cloud
* displayConcurrentConnections: if set to true, we can view the number of concurrent connections the sink receives from dispatcher/adapter. This is useful if we want to debug performance problem. We can view the number of concurrent connections by viewing the sink log. Eg. if we saw the sink currently has 50 connections, that means the sink is currently receiving events from 50 partitions from the adapter/dispatcher concurrently. This is useful for debugging adapter/dispatcher threading problem.
* cloudevent: set to true if we want the sink to receive events as cloud event, otherwise the sink receives the events as http requests. 
* forward: set to true if we want the sink to forward events to scraper 
* print: set to true so the sink prints out all the source/partition/offset. 
* delay: sink delay in seconds eg. 3 -> 3 seconds 

Then you can create application as the following: 

ibmcloud ce app create -n kafkasink -i stevendongatibm/newdisplay:3 --env scraper="https://scraper.nl51m5ac5j5.dev-pg1.codeengine.dev.appdomain.cloud" --env displayConcurrentConnections=true --env cloudevent=true --env forward=true --env delay=3 --env print=true