#!/bin/bash

python3 manage.py makemigrations --no-input
python3 manage.py migrate
python3 manage.py collectstatic --no-input
gunicorn --bind 0.0.0.0:8000 dashboard.wsgi