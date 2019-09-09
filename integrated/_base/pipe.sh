#!/bin/bash

function uncolor()
{
	sed 's/\x1B\[[0-9;]\+[A-Za-z]//g'
}
export -f uncolor
