#!/usr/bin/env python
import yaml, os

db = yaml.load(
    open(
        "{0}/{1}".format(
            os.getenv("HOME"), 
            ".tpp/database.yml"
        ), 
        "r"
    ),
    Loader=yaml.FullLoader
)

globus = yaml.load(
    open(
        "{0}/{1}".format(
            os.getenv("HOME"),
            ".tpp/globus.yml"
        ),
        "r"
    ),
    Loader=yaml.FullLoader
)
