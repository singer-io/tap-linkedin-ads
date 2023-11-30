.DEFAULT_GOAL := lint

lint:
	pylint tap_linkedin_ads -d 'broad-except,line-too-long,missing-class-docstring,missing-function-docstring,missing-module-docstring,too-many-locals,invalid-name,consider-using-f-string,use-list-literal,use-dict-literal,raise-missing-from, unspecified-encoding, broad-exception-raised'
