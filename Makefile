SLANGC ?= slangc

.PHONY: test live
test:
	$(SLANGC) test src/sqs

# The client against ElasticMQ in Docker (needs docker).
live:
	SLANGC="$(SLANGC)" sh ./tests/live/run.sh
