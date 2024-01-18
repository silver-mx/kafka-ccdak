#!/usr/bin/env bash

# Converts the scripts to UNIX end line char (\r\n => \n)
#sudo apt-get install -y dos2unix # Installs dos2unix Linux
find . -type f -name "*.sh" -exec dos2unix {} \;