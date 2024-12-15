# Compiler and flags
CC = gcc
CFLAGS = -Wall -Wextra -O2
LDFLAGS = -lsqlite3

# Targets
TARGETS = populate_db join_sim_hash join_sim_ttj

# Source files
SRC_POPULATE = populate_db.c
SRC_JOIN_SIM = join_sim.c

# Database file
DB_FILE = test_db.db

# Default target
all: $(TARGETS)

# Compile the populate_db program
populate_db: $(SRC_POPULATE)
	$(CC) $(CFLAGS) $(SRC_POPULATE) -o populate_db $(LDFLAGS)

# Compile the join_sim program with Hash Join
join_sim_hash: $(SRC_JOIN_SIM)
	$(CC) $(CFLAGS) -DJOIN_TYPE=0 $(SRC_JOIN_SIM) -o join_sim_hash $(LDFLAGS)

# Compile the join_sim program with TTJ
join_sim_ttj: $(SRC_JOIN_SIM)
	$(CC) $(CFLAGS) -DJOIN_TYPE=1 $(SRC_JOIN_SIM) -o join_sim_ttj $(LDFLAGS)

# Run the database population
populate: populate_db
	@echo "[INFO] Running populate_db to populate $(DB_FILE)..."
	./populate_db

# Run the join simulation with Hash Join
join_hash: join_sim_hash
	@echo "[INFO] Running join_sim_hash (Hash Join) on $(DB_FILE)..."
	./join_sim_hash

# Run the join simulation with TTJ
join_ttj: join_sim_ttj
	@echo "[INFO] Running join_sim_ttj (TTJ) on $(DB_FILE)..."
	./join_sim_ttj

# Clean up generated files
clean:
	@echo "[INFO] Cleaning up..."
	rm -f $(TARGETS) $(DB_FILE)

# Phony targets
.PHONY: all populate join_hash join_ttj clean
