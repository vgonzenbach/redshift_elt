#!/bin/bash

.venv/bin/python pipeline/setup_redshift.py
.venv/bin/python pipeline/create_tables.py
.venv/bin/python pipeline/elt.py