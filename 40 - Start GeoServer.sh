#!/usr/bin/bash


echo ""
echo ""
echo "Starting the open source GeoServer on localhost:8080 .."
echo ""
echo "   http://localhost:8080/geoserver/web/?0"
echo "   (admin, geoserver)"
echo ""
echo ""

export GEOSERVER_HOME=/opt/geoserver
export JAVA_OPTS="${JAVA_OPTS} -DENABLE_JSONP=true"
   #
cd ${GEOSERVER_HOME}/bin

startup.sh

echo ""
echo ""
