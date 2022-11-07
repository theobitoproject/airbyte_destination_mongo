.PHONY: check
check:
	go run -race . check --config secrets/config.json


.PHONY: write
write:
	go run -race . write --config secrets/config.json --catalog sample_files/configured_catalog.json
