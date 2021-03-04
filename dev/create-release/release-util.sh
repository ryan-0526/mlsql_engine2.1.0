#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

DRY_RUN=${DRY_RUN:-0}
MLSQL_HTTPS_REPO="https://github.com/allwefantasy/streamingpro.git"
MLSQL_GIT_REPO="git@github.com:allwefantasy/streamingpro.git"
MLSQL_REPO_UI="https://github.com/allwefantasy/streamingpro"




function error {
  echo "$*"
  exit 1
}

function read_config {
  local PROMPT="$1"
  local DEFAULT="$2"
  local REPLY=

  read -p "$PROMPT [$DEFAULT]: " REPLY
  local RETVAL="${REPLY:-$DEFAULT}"
  if [ -z "$RETVAL" ]; then
    error "$PROMPT is must be provided."
  fi
  echo "$RETVAL"
}

function parse_version {
  grep -e '<version>.*</version>' | \
    head -n 2 | tail -n 1 | cut -d'>' -f2 | cut -d '<' -f1
}

function run_silent {
  local BANNER="$1"
  local LOG_FILE="$2"
  shift 2

  echo "========================"
  echo "= $BANNER"
  echo "Command: $@"
  echo "Log file: $LOG_FILE"

  "$@" 1>"$LOG_FILE" 2>&1

  local EC=$?
  if [ $EC != 0 ]; then
    echo "Command FAILED. Check full logs for details."
    tail "$LOG_FILE"
    exit $EC
  fi
}

function fcreate_secure {
  local FPATH="$1"
  rm -f "$FPATH"
  touch "$FPATH"
  chmod 600 "$FPATH"
}

function check_for_tag {
  curl -s --head --fail "https://github.com/allwefantasy/streamingpro/blob/$1/pom.xml" >/dev/null
}

function get_release_info {
  if [ -z "$GIT_BRANCH" ]; then
    # If no branch is specified, found out the latest branch from the repo.
    GIT_BRANCH=$(git ls-remote --heads "$MLSQL_REPO" |
      grep -v refs/heads/master |
      awk '{print $2}' |
      sort -r |
      head -n 1 |
      cut -d/ -f3)
  fi

  export GIT_BRANCH=$(read_config "Branch" "$GIT_BRANCH")

  # Find the current version for the branch.
  local VERSION=$(curl -s "$MLSQL_REPO_UI/blob/$GIT_BRANCH/pom.xml" |
    parse_version)
  echo "Current branch version is $VERSION."

  if [[ ! $VERSION =~ .*-SNAPSHOT ]]; then
    error "Not a SNAPSHOT version: $VERSION"
  fi

  NEXT_VERSION="$VERSION"
  RELEASE_VERSION="${VERSION/-SNAPSHOT/}"
  SHORT_VERSION=$(echo "$VERSION" | cut -d . -f 1-2)
  local REV=$(echo "$VERSION" | cut -d . -f 3)

  # Find out what rc is being prepared.
  # - If the current version is "x.y.0", then this is rc1 of the "x.y.0" release.
  # - If not, need to check whether the previous version has been already released or not.
  #   - If it has, then we're building rc1 of the current version.
  #   - If it has not, we're building the next RC of the previous version.
  local RC_COUNT
  if [ $REV != 0 ]; then
    local PREV_REL_REV=$((REV - 1))
    local PREV_REL_TAG="v${SHORT_VERSION}.${PREV_REL_REV}"
    if check_for_tag "$PREV_REL_TAG"; then
      RC_COUNT=1
      REV=$((REV + 1))
      NEXT_VERSION="${SHORT_VERSION}.${REV}-SNAPSHOT"
    else
      RELEASE_VERSION="${SHORT_VERSION}.${PREV_REL_REV}"
      RC_COUNT=$(git ls-remote --tags "$ASF_REPO" "v${RELEASE_VERSION}-rc*" | wc -l)
      RC_COUNT=$((RC_COUNT + 1))
    fi
  else
    REV=$((REV + 1))
    NEXT_VERSION="${SHORT_VERSION}.${REV}-SNAPSHOT"
    RC_COUNT=1
  fi

  export NEXT_VERSION
  export RELEASE_VERSION=$(read_config "Release" "$RELEASE_VERSION")

  RC_COUNT=$(read_config "RC #" "$RC_COUNT")

  # Check if the RC already exists, and if re-creating the RC, skip tag creation.
  RELEASE_TAG="v${RELEASE_VERSION}-rc${RC_COUNT}"
  SKIP_TAG=0
  if check_for_tag "$RELEASE_TAG"; then
    read -p "$RELEASE_TAG already exists. Continue anyway [y/n]? " ANSWER
    if [ "$ANSWER" != "y" ]; then
      error "Exiting."
    fi
    SKIP_TAG=1
  fi


  export RELEASE_TAG

  GIT_REF="$RELEASE_TAG"
  if is_dry_run; then
    echo "This is a dry run. Please confirm the ref that will be built for testing."
    GIT_REF=$(read_config "Ref" "$GIT_REF")
  fi
  export GIT_REF
  export SPARK_PACKAGE_VERSION="$RELEASE_TAG"


  GIT_NAME=$(git config user.name || echo "")
  export GIT_NAME=$(read_config "Full name" "$GIT_NAME")

  cat <<EOF
================
Release details:
BRANCH:     $GIT_BRANCH
VERSION:    $RELEASE_VERSION
TAG:        $RELEASE_TAG
NEXT:       $NEXT_VERSION
FULL NAME:  $GIT_NAME
E-MAIL:     $GIT_EMAIL
================
EOF

  read -p "Is this info correct [y/n]? " ANSWER
  if [ "$ANSWER" != "y" ]; then
    echo "Exiting."
    exit 1
  fi

}

function is_dry_run {
  [[ $DRY_RUN = 1 ]]
}

# Initializes JAVA_VERSION to the version of the JVM in use.
function init_java {
  if [ -z "$JAVA_HOME" ]; then
    error "JAVA_HOME is not set."
  fi
  JAVA_VERSION=$("${JAVA_HOME}"/bin/javac -version 2>&1 | cut -d " " -f 2)
  export JAVA_VERSION
}

# Initializes MVN_EXTRA_OPTS and SBT_OPTS depending on the JAVA_VERSION in use. Requires init_java.
function init_maven_sbt {
  MVN="mvn"
  export MVN
}