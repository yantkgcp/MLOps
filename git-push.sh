#!/bin/bash

python kernel.py
git add .
git status
git commit -am "update"
git push -u origin
