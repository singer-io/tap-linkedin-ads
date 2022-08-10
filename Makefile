.DEFAULT_GOAL := lint

lint:pylint tap_linkedin_ads -d 'broad-except,chained-comparison,line-too-long,missing-class-docstring,missing-function-docstring,missing-module-docstring,too-many-locals'
