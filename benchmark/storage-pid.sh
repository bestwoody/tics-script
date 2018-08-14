#!/bin/bash

ps -ef | grep 'theflash server' | grep -v grep | grep -v ssh | grep -v nohup | awk '{print $2}'
