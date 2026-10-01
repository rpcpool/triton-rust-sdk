YELLOWSTONE_GRPC_DIR := yellowstone-account-sync-proto/proto/yellowstone-grpc
YELLOWSTONE_GRPC_TAG := client-v13.3.0

.PHONY: setup-submodules
setup-submodules:
	git submodule update --init $(YELLOWSTONE_GRPC_DIR)
	git -C $(YELLOWSTONE_GRPC_DIR) fetch --tags
	git -C $(YELLOWSTONE_GRPC_DIR) checkout $(YELLOWSTONE_GRPC_TAG)
	git -C $(YELLOWSTONE_GRPC_DIR) sparse-checkout init --cone
	git -C $(YELLOWSTONE_GRPC_DIR) sparse-checkout set yellowstone-grpc-proto
	find "$(YELLOWSTONE_GRPC_DIR)" -name .git -prune -o -type f ! -name '*.proto' -exec rm -f {} +

clean:
	rm -rf target
