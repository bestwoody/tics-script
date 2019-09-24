#!/bin/bash

ps -ef | grep 'flash server' | grep -v grep | grep -v ssh | grep -v nohup | awk '{print $2}'
