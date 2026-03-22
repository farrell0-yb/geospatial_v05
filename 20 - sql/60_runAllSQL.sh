#!/bin/bash


#  60_runAllSQL.sh
#
#  Runs all SQL files in numeric order against the YugabyteDB database
#  specified in ../properties.ini using ysqlsh.
#
#  Usage:  bash 60_runAllSQL.sh
#          (run from the "20 - sql" directory, or from the project root)
#

set -e

# Locate project root and properties file
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROPERTIES_FILE="$PROJECT_ROOT/properties.ini"

if [ ! -f "$PROPERTIES_FILE" ]; then
   echo "ERROR: Cannot find properties.ini at $PROPERTIES_FILE"
   exit 1
fi


# Parse properties.ini
#

DATABASE_HOST=$(grep -E '^DATABASE_HOST=' "$PROPERTIES_FILE" | cut -d'=' -f2 | tr -d '[:space:]')
DATABASE_PORT=$(grep -E '^DATABASE_PORT=' "$PROPERTIES_FILE" | cut -d'=' -f2 | tr -d '[:space:]')
DATABASE_NAME=$(grep -E '^DATABASE_NAME=' "$PROPERTIES_FILE" | cut -d'=' -f2 | tr -d '[:space:]')
DATABASE_USER=$(grep -E '^DATABASE_USER=' "$PROPERTIES_FILE" | cut -d'=' -f2 | tr -d '[:space:]')


echo ""
echo ""
echo "Target: $DATABASE_USER@$DATABASE_HOST:$DATABASE_PORT/$DATABASE_NAME"
echo ""

# cd into the SQL directory (required for \copy relative paths)
#

cd "$SCRIPT_DIR"


# SQL files in execution order
#

SQL_FILES=(
   "10_CreateGeometryType.sql"
   "11_CreateSchema.sql"
   "12_CreateGeographyType.sql"
   "15_LoadData.sql"
   "20_GeohashFunctions.sql"
   "25_GeometryFunctions.sql"
   "26_Tier1_GeometryFunctions.sql"
   "27_Tier2_GeometryFunctions.sql"
   "28_Tier3_GeometryFunctions.sql"
   "30_GeohashPolygonFunctions.sql"
   "31_GeohashBboxFunctions.sql"
   "35_TestQueries.sql"
   )


# Run each file
#

for SQL_FILE in "${SQL_FILES[@]}"
   do

   if [ ! -f "$SQL_FILE" ]; then
      echo "WARNING: $SQL_FILE not found, skipping .."
      continue
   fi

   echo "Running: $SQL_FILE .."

   ysqlsh \
      -h "$DATABASE_HOST" \
      -p "$DATABASE_PORT" \
      -d "$DATABASE_NAME" \
      -U "$DATABASE_USER" \
      -f "$SQL_FILE" \
      -v ON_ERROR_STOP=1

   echo "Done: $SQL_FILE .."
   echo ""

   done

echo ""
echo ""
echo "All SQL files executed successfully .."
echo ""
echo ""
