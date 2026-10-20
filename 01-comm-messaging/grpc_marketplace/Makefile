# Variables
PROTO_DIR=.
OUT_DIR=.

# PROTO files
PROTO_FILES=$(wildcard $(PROTO_DIR)/market_service.proto $(PROTO_DIR)/notify_service.proto)

# Generated Python files
GEN_PY=$(PROTO_FILES:.proto=_pb2.py) $(PROTO_FILES:.proto=_pb2_grpc.py)

# gRPC Python plugin
PYTHON_PLUGIN=python3 -m grpc_tools.protoc

.PHONY: all clean

all: $(GEN_PY)

# Rule for generating Python code from PROTO files
%_pb2.py %_pb2_grpc.py: %.proto
	$(PYTHON_PLUGIN) -I$(PROTO_DIR) --python_out=$(OUT_DIR) --grpc_python_out=$(OUT_DIR) $<

# Clean up generated files
clean:
	rm -f $(GEN_PY)
