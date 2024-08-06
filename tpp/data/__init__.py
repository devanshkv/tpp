#!/usr/bin/env python
import yaml, os

db = yaml.load(
    open(
        "{0}/{1}".format(
            os.path.dirname(__file__), 
            "database.yml"
        ), 
        "r"
    ),
    Loader=yaml.FullLoader
)

globus = yaml.load(
    open(
        "{0}/{1}".format(
            os.path.dirname(__file__),
            "globus.yml"
        ),
        "r"
    ),
    Loader=yaml.FullLoader
)
