#!/usr/bin/env bash
docker rm smart-city-app
docker rmi smart-city-image
docker build --no-cache -t smart-city-image .
CID=$(docker run -d -it -v $(pwd)/csv_output:/usr/src/app/data --name smart-city-app smart-city-image)
docker logs -f $CID

