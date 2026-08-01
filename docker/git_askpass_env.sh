#!/bin/sh
# Git calls this for username/password prompts
case "$1" in
  *sername*) echo "${GIT_PUSH_USERNAME:-git}" ;;
  *assword*) echo "${GIT_PUSH_TOKEN:-${GITHUB_TOKEN:-${GIT_TOKEN:-}}}" ;;
esac
