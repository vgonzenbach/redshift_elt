#!/bin/bash

.venv/bin/python setup_redshift.py
.venv/bin/python create_tables.py
.venv/bin/python etl.py