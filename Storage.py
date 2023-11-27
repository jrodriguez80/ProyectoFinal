from flask import Flask, request
import json
from typing import List

app = Flask(__name__)
replicas = []


