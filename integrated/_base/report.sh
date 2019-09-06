#!/bin/bash

function to_table()
{
	python "${integrated}"/_base/to_table.py "${@}"
}
export -f to_table
