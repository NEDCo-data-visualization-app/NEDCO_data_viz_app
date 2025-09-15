from flask import Blueprint
bp = Blueprint("main", __name__, template_folder="../../templates", static_folder="../../static")
# routes will be generated into this package by the refactor script
