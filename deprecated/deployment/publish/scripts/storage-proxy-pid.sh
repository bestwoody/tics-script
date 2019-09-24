#!/bin/bash

ps -ef | grep 'tiflash-proxy' | grep -v grep | grep -v ssh | grep -v nohup | awk '{print $2}'
