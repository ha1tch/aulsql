# E-Commerce Example for aul
# 
# Usage:
#   make setup    - First-time setup (build + init + migrate + seed)
#   make run      - Start the server
#   make reset    - Wipe and reinitialize everything
#   make clean    - Remove database and build artifacts

# Configuration
DB_FILE := ecommerce.db
TDS_PORT := 1433
HTTP_PORT := 8080
PROCEDURES_DIR := procedures

# iaul connection (aul accepts any credentials)
USER := sa
PASSWORD := password
DATABASE := master

# Paths (relative to this directory)
AUL_ROOT := ../..
AUL := $(AUL_ROOT)/aul
IAUL := $(AUL_ROOT)/iaul

.PHONY: all setup run reset clean build init help shell

help:
	@echo "E-Commerce Example"
	@echo ""
	@echo "Commands:"
	@echo "  make setup    - First-time setup (build + init + migrate + seed)"
	@echo "  make run      - Start the aul server (foreground)"
	@echo "  make reset    - Wipe database and reinitialize"
	@echo "  make clean    - Remove database file"
	@echo "  make shell    - Interactive SQL shell (requires server running)"
	@echo ""
	@echo "Connection:"
	@echo "  Host: localhost"
	@echo "  Port: $(TDS_PORT)"
	@echo "  Database: $(DATABASE)"
	@echo "  User: $(USER)"
	@echo "  Password: $(PASSWORD)"

all: help

# Build aul and iaul if not present
build:
	@if [ ! -f "$(AUL)" ]; then \
		echo "Building aul..."; \
		cd $(AUL_ROOT) && make build; \
	fi
	@if [ ! -f "$(IAUL)" ]; then \
		echo "Building iaul..."; \
		cd $(AUL_ROOT) && make build-iaul; \
	fi

# First-time setup - starts server, runs migrations, stops server
setup: build init
	@echo "Starting server for migration..."
	@$(AUL) \
		--storage sqlite \
		--storage-path $(DB_FILE) \
		-d $(PROCEDURES_DIR) \
		--tds-port $(TDS_PORT) \
		--http-port $(HTTP_PORT) \
		--no-banner &
	@sleep 2
	@echo "Running schema migration..."
	@$(IAUL) -host localhost -port $(TDS_PORT) -user $(USER) -password $(PASSWORD) -database $(DATABASE) -f ecommerce_schema.sql -quiet \
		|| (pkill -f "aul.*$(DB_FILE)" 2>/dev/null; exit 1)
	@echo "Loading sample data..."
	@$(IAUL) -host localhost -port $(TDS_PORT) -user $(USER) -password $(PASSWORD) -database $(DATABASE) -f seed_data.sql -quiet \
		|| (pkill -f "aul.*$(DB_FILE)" 2>/dev/null; exit 1)
	@pkill -f "aul.*$(DB_FILE)" 2>/dev/null || true
	@sleep 1
	@echo ""
	@echo "========================================="
	@echo "Setup complete!"
	@echo ""
	@echo "To start the server:"
	@echo "  make run"
	@echo ""
	@echo "Then connect with any SQL Server client:"
	@echo "  Host: localhost"
	@echo "  Port: $(TDS_PORT)"
	@echo "  User: $(USER)"
	@echo "  Password: $(PASSWORD)"
	@echo "========================================="

# Initialize empty database
init:
	@echo "Initializing database..."
	@rm -f $(DB_FILE)

# Start the server (foreground)
run: build
	@echo "Starting aul server..."
	@echo "  Database: $(DB_FILE)"
	@echo "  TDS Port: $(TDS_PORT)"
	@echo "  HTTP Port: $(HTTP_PORT)"
	@echo "  Procedures: $(PROCEDURES_DIR)"
	@echo ""
	@$(AUL) \
		--storage sqlite \
		--storage-path $(DB_FILE) \
		-d $(PROCEDURES_DIR) \
		--tds-port $(TDS_PORT) \
		--http-port $(HTTP_PORT)

# Interactive shell (requires server running)
shell: build
	@$(IAUL) -host localhost -port $(TDS_PORT) -user $(USER) -password $(PASSWORD) -database $(DATABASE)

# Reset everything
reset: clean setup

# Clean up
clean:
	@echo "Cleaning up..."
	@rm -f $(DB_FILE)
	@echo "Done."
