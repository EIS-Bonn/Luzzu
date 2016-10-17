#!/bin/bash

gunzip -c $1 | sed -n "$2,$3p"